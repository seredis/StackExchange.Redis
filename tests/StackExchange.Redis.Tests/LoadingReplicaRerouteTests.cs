using System;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using StackExchange.Redis.Server;
using Xunit;

namespace StackExchange.Redis.Tests;

/// <summary>
/// A replica that answers <c>LOADING</c> cannot serve, even though the socket is up. <c>PreferReplica</c>
/// reads must move on to a replica that can, and from there to the primary when every replica is loading
/// (a simultaneous replica upgrade leaves the primary as the only node that can answer). See #1913.
/// </summary>
[RunPerProtocol]
public class LoadingReplicaRerouteTests(ITestOutputHelper log)
{
    private sealed class PrimaryAndReplicasServer : InProcessTestServer
    {
        private readonly Server.RedisServer.Node _primary;
        private readonly Server.RedisServer.Node _replicaA;
        private readonly Server.RedisServer.Node _replicaB;
        private int _primaryGets;
        private int _replicaGets;

        public EndPoint ReplicaA { get; }

        public PrimaryAndReplicasServer(ITestOutputHelper log)
            : base(log)
        {
            ReplicaA = AddEmptyNode();
            var replicaB = AddEmptyNode();
            SetReplicaOf(ReplicaA, DefaultEndPoint);
            SetReplicaOf(replicaB, DefaultEndPoint);
            Assert.True(TryGetNode(DefaultEndPoint, out _primary!));
            Assert.True(TryGetNode(ReplicaA, out _replicaA!));
            Assert.True(TryGetNode(replicaB, out _replicaB!));
        }

        public int PrimaryGets => Volatile.Read(ref _primaryGets);

        public int ReplicaGets => Volatile.Read(ref _replicaGets);

        public void SetReplicasLoading(bool loading)
        {
            _replicaA.IsLoading = loading;
            _replicaB.IsLoading = loading;
        }

        public override TypedRedisValue Execute(RedisClient client, in RedisRequest request)
        {
            var result = base.Execute(client, in request);
            if (request.KnownCommand == RedisCommand.GET && !result.IsError)
            {
                if (ReferenceEquals(client.Node, _primary))
                {
                    Interlocked.Increment(ref _primaryGets);
                }
                else if (ReferenceEquals(client.Node, _replicaA) || ReferenceEquals(client.Node, _replicaB))
                {
                    Interlocked.Increment(ref _replicaGets);
                }
            }

            return result;
        }
    }

    [Fact]
    public async Task PreferReplicaReadUsesPrimaryWhenEveryReplicaIsLoading()
    {
        using var server = new PrimaryAndReplicasServer(log);
        await using var conn = await server.ConnectAsync();
        var db = conn.GetDatabase();

        Assert.False(conn.GetServer(server.DefaultEndPoint).IsReplica);
        Assert.Equal(2, CountReplicas(conn));

        var key = Me();
        await db.StringSetAsync(key, "from-primary");

        // the replicas are the preferred target while they can serve
        Assert.Equal("from-primary", await db.StringGetAsync(key, CommandFlags.PreferReplica));
        Assert.True(server.ReplicaGets > 0);
        var replicaGetsWhileHealthy = server.ReplicaGets;

        server.SetReplicasLoading(true);

        // both replicas answer LOADING; the read still returns, served by the primary
        Assert.Equal("from-primary", await db.StringGetAsync(key, CommandFlags.PreferReplica));
        Assert.True(server.PrimaryGets > 0);

        var replicaGetsAfterReroute = server.ReplicaGets;
        var primaryGetsAfterReroute = server.PrimaryGets;
        Assert.Equal("from-primary", await db.StringGetAsync(key, CommandFlags.PreferReplica));

        // the loading replicas stay out of selection; this read does not touch them
        Assert.Equal(replicaGetsAfterReroute, server.ReplicaGets);
        Assert.True(server.PrimaryGets > primaryGetsAfterReroute);

        // INFO is served while loading, and a successful INFO must not make the replica selectable again
        var info = await conn.GetServer(server.ReplicaA).InfoAsync("replication");
        Assert.Contains(info, static section => section.Key.Equals("Replication", StringComparison.OrdinalIgnoreCase));
        Assert.Equal("from-primary", await db.StringGetAsync(key, CommandFlags.PreferReplica));
        Assert.Equal(replicaGetsAfterReroute, server.ReplicaGets);

        server.SetReplicasLoading(false);

        // once a replica can answer again it is eligible, and a later read uses one
        var deadline = DateTime.UtcNow + TimeSpan.FromSeconds(8);
        while (server.ReplicaGets == replicaGetsAfterReroute && DateTime.UtcNow < deadline)
        {
            Assert.Equal("from-primary", await db.StringGetAsync(key, CommandFlags.PreferReplica));
            await Task.Delay(50);
        }

        Assert.True(server.ReplicaGets > replicaGetsWhileHealthy);
        Assert.True(server.ReplicaGets > replicaGetsAfterReroute);
    }

    [Fact]
    public async Task LoadingPrimaryStillSurfacesTheError()
    {
        using var server = new InProcessTestServer(log);
        await using var conn = await server.ConnectAsync();
        server.IsLoading = true;

        var ex = await Assert.ThrowsAsync<RedisServerException>(() => conn.GetDatabase().StringGetAsync(Me()));
        Assert.Equal(RedisErrorKind.Loading, ex.Kind);
    }

    private static int CountReplicas(ConnectionMultiplexer conn)
    {
        var count = 0;
        foreach (var server in conn.GetServers())
        {
            if (server.IsReplica) count++;
        }

        return count;
    }

    private static RedisKey Me([System.Runtime.CompilerServices.CallerMemberName] string name = "") => name;
}
