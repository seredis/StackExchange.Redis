using System;
using System.Diagnostics;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using StackExchange.Redis.Server;
using Xunit;

namespace StackExchange.Redis.Tests;

/// <summary>
/// A publish to a subscribed channel goes to the server the channel is subscribed on - but not when that server no longer
/// satisfies the caller's primary/replica demand, as after a role change, since the subscription itself stays put (#3357).
/// </summary>
[RunPerProtocol]
public class PublishRoutingUnitTests(ITestOutputHelper log)
{
    private sealed class RecordingServer : InProcessTestServer
    {
        public EndPoint Replica { get; }
        private object? _lastPublishNode;

        public RecordingServer(ITestOutputHelper log) : base(log)
        {
            Replica = AddEmptyNode();
            SetReplicaOf(Replica, DefaultEndPoint);
        }

        protected override TypedRedisValue Publish(RedisClient client, in RedisRequest request)
        {
            Volatile.Write(ref _lastPublishNode, client.Node);
            return base.Publish(client, request);
        }

        public bool LastPublishWasOn(EndPoint endpoint)
            => TryGetNode(endpoint, out var node) && ReferenceEquals(Volatile.Read(ref _lastPublishNode), node);

        // the replica is promoted, and the old primary follows it; nothing is announced and no connection drops
        public void SwapRoles()
        {
            SetReplicaOf(Replica, null!);
            SetReplicaOf(DefaultEndPoint, Replica);
        }
    }

    private static async Task<ConnectionMultiplexer> ConnectAndSubscribeAsync(RecordingServer server, RedisChannel channel, CommandFlags subscribeFlags)
    {
        var conn = await server.ConnectAsync();
        Assert.False(conn.GetServer(server.DefaultEndPoint).IsReplica);
        Assert.True(conn.GetServer(server.Replica).IsReplica);

        await conn.GetSubscriber().SubscribeAsync(channel, static (_, _) => { }, subscribeFlags);
        Assert.Equal(server.DefaultEndPoint, conn.GetSubscriber().SubscribedEndpoint(channel));
        return conn;
    }

    private static async Task SwapRolesAndWaitForClientAsync(RecordingServer server, ConnectionMultiplexer conn)
    {
        server.SwapRoles();
        await conn.ConfigureAsync();

        var timer = Stopwatch.StartNew();
        while (!(conn.GetServer(server.DefaultEndPoint).IsReplica && !conn.GetServer(server.Replica).IsReplica))
        {
            if (timer.Elapsed > TimeSpan.FromSeconds(10)) Assert.Fail("the client did not see the role change");
            await Task.Delay(50);
        }
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task DemandMasterPublishFollowsTheNewPrimary(bool viaDatabase)
    {
        using var server = new RecordingServer(log);
        var channel = RedisChannel.Literal(nameof(DemandMasterPublishFollowsTheNewPrimary));
        await using var conn = await ConnectAndSubscribeAsync(server, channel, CommandFlags.DemandMaster);

        await SwapRolesAndWaitForClientAsync(server, conn);

        // the subscription stays where it was made...
        Assert.Equal(server.DefaultEndPoint, conn.GetSubscriber().SubscribedEndpoint(channel));

        // ...but a publish that demands a primary goes to the primary, rather than failing on the now-replica
        if (viaDatabase)
        {
            await conn.GetDatabase().PublishAsync(channel, "async", CommandFlags.DemandMaster);
            Assert.True(server.LastPublishWasOn(server.Replica));
            conn.GetDatabase().Publish(channel, "sync", CommandFlags.DemandMaster);
        }
        else
        {
            await conn.GetSubscriber().PublishAsync(channel, "async", CommandFlags.DemandMaster);
            Assert.True(server.LastPublishWasOn(server.Replica));
            conn.GetSubscriber().Publish(channel, "sync", CommandFlags.DemandMaster);
        }
        Assert.True(server.LastPublishWasOn(server.Replica));
    }

    [Fact]
    public async Task DemandReplicaPublishDoesNotUseAPrimarySubscription()
    {
        using var server = new RecordingServer(log);
        var channel = RedisChannel.Literal(nameof(DemandReplicaPublishDoesNotUseAPrimarySubscription));
        await using var conn = await ConnectAndSubscribeAsync(server, channel, CommandFlags.DemandMaster);

        await conn.GetSubscriber().PublishAsync(channel, "replica", CommandFlags.DemandReplica);
        Assert.True(server.LastPublishWasOn(server.Replica));
    }

    [Fact]
    public async Task UndemandingPublishStillUsesTheSubscribedServer()
    {
        using var server = new RecordingServer(log);
        var channel = RedisChannel.Literal(nameof(UndemandingPublishStillUsesTheSubscribedServer));
        await using var conn = await ConnectAndSubscribeAsync(server, channel, CommandFlags.DemandMaster);

        await SwapRolesAndWaitForClientAsync(server, conn);

        // unchanged behavior: without a demand, the subscribed server is used, even though it is now a replica
        await conn.GetSubscriber().PublishAsync(channel, "default");
        Assert.True(server.LastPublishWasOn(server.DefaultEndPoint));
    }
}
