using System;
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
        private int _replicaALoading;
        private int _replicaBLoading;
        private int _primaryGets;
        private int _replicaGets;

        public PrimaryAndReplicasServer(ITestOutputHelper log)
            : base(log)
        {
            var replicaA = AddEmptyNode();
            var replicaB = AddEmptyNode();
            SetReplicaOf(replicaA, DefaultEndPoint);
            SetReplicaOf(replicaB, DefaultEndPoint);
            Assert.True(TryGetNode(DefaultEndPoint, out _primary!));
            Assert.True(TryGetNode(replicaA, out _replicaA!));
            Assert.True(TryGetNode(replicaB, out _replicaB!));
        }

        public int PrimaryGets => Volatile.Read(ref _primaryGets);

        public int ReplicaGets => Volatile.Read(ref _replicaGets);

        public void SetReplicasLoading(bool loading)
        {
            Volatile.Write(ref _replicaALoading, loading ? 1 : 0);
            Volatile.Write(ref _replicaBLoading, loading ? 1 : 0);
        }

        public override TypedRedisValue Execute(RedisClient client, in RedisRequest request)
        {
            if (NodeIsLoading(client.Node))
            {
                return TypedRedisValue.Error("LOADING Redis is loading the dataset in memory");
            }

            if (request.KnownCommand == RedisCommand.GET)
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

            return base.Execute(client, in request);
        }

        private bool NodeIsLoading(Server.RedisServer.Node? node)
            => (ReferenceEquals(node, _replicaA) && Volatile.Read(ref _replicaALoading) != 0)
               || (ReferenceEquals(node, _replicaB) && Volatile.Read(ref _replicaBLoading) != 0);
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
