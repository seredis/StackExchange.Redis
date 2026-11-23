using System;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using StackExchange.Redis.Server;
using Xunit;

namespace StackExchange.Redis.Tests;

[RunPerProtocol]
public class SubscriptionRestorationTests(ITestOutputHelper output)
{
    [Theory]
    [InlineData(false, false)]
    [InlineData(true, false)]
    [InlineData(false, true)]
    [InlineData(true, true)]
    public async Task OverlappingRestoresCannotSubscribeOnTwoServers(bool pattern, bool fireAndForget)
    {
        using var server = new PausedSubscribeServer(output);
        await using var connection = await server.ConnectAsync();
        var subscriber = connection.DefaultSubscriber;
        var channel = pattern ? RedisChannel.Pattern("restoration.*") : RedisChannel.Literal("restoration.channel");
        var subscription = new ConnectionMultiplexer.SingleNodeSubscription(fireAndForget ? CommandFlags.FireAndForget : CommandFlags.None);
        var firstServer = connection.GetServerEndPoint(server.DefaultEndPoint, ServerProvenance.Configured);
        var secondServer = connection.GetServerEndPoint(server.SecondEndPoint, ServerProvenance.Configured);
        var first = subscription.EnsureSubscribedToServerAsync(subscriber, channel,
            fireAndForget ? CommandFlags.FireAndForget : CommandFlags.None, true, firstServer);
        try
        {
            await server.FirstSubscribe.Task.WithTimeout(5000);
            var second = subscription.EnsureSubscribedToServerAsync(subscriber, channel, CommandFlags.None, true, secondServer);
            var ping = Message.Create(-1, CommandFlags.None, RedisCommand.PING);
            ping.SetForSubscriptionBridge();
            await connection.ExecuteAsyncImpl(ping, ResultProcessor.ResponseTimer, null, secondServer).WithTimeout(5000);
            Assert.Equal(0, Volatile.Read(ref server.SecondSubscribeCount));
            server.Release.Set();
            await Task.WhenAll(first, second).WithTimeout(5000);
            Assert.True(await Poll.UntilAsync(() => subscription.IsConnectedAny(), timeoutMilliseconds: 5000));
            Assert.Equal(1, Volatile.Read(ref server.FirstSubscribeCount));
            Assert.Equal(0, Volatile.Read(ref server.SecondSubscribeCount));
            Assert.Same(firstServer, subscription.GetAnyCurrentServer());
        }
        finally
        {
            server.Release.Set();
            try { await first.WithTimeout(5000); } catch { }
        }
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task LateAcknowledgementCannotReplaceANewerSubscription(bool pattern)
    {
        using var server = new PausedSubscribeServer(output);
        var config = server.GetClientConfig();
        config.AsyncTimeout = 200;
        config.HeartbeatInterval = TimeSpan.FromMilliseconds(20);
        await using var connection = await ConnectionMultiplexer.ConnectAsync(config);
        var channel = pattern ? RedisChannel.Pattern("restoration.*") : RedisChannel.Literal("restoration.channel");
        var subscription = new ConnectionMultiplexer.SingleNodeSubscription(CommandFlags.None);
        var firstServer = connection.GetServerEndPoint(server.DefaultEndPoint, ServerProvenance.Configured);
        var secondServer = connection.GetServerEndPoint(server.SecondEndPoint, ServerProvenance.Configured);
        var first = subscription.EnsureSubscribedToServerAsync(connection.DefaultSubscriber, channel, CommandFlags.None, true, firstServer);
        try
        {
            await server.FirstSubscribe.Task.WithTimeout(5000);
            await Assert.ThrowsAsync<RedisTimeoutException>(() => first.WithTimeout(5000));
            await subscription.EnsureSubscribedToServerAsync(connection.DefaultSubscriber, channel, CommandFlags.None, true, secondServer).WithTimeout(5000);
            Assert.Same(secondServer, subscription.GetAnyCurrentServer());
            server.Release.Set();
            await server.FirstUnsubscribe.Task.WithTimeout(5000);
            Assert.Same(secondServer, subscription.GetAnyCurrentServer());
            Assert.Equal(1, subscription.GetConnectionCount());
        }
        finally { server.Release.Set(); }
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task DeferredRestoreIsRedrivenWhenTheInFlightAttemptFails(bool pattern)
    {
        // an internal restore that backs off in favour of an in-flight attempt is owed a retry if that
        // attempt fails: the restore paths are event-driven, so nothing else would bring the subscription back
        using var server = new PausedSubscribeServer(output) { FailFirstSubscribe = true };
        await using var connection = await server.ConnectAsync();
        var subscriber = connection.DefaultSubscriber;
        var channel = pattern ? RedisChannel.Pattern("restoration.*") : RedisChannel.Literal("restoration.channel");
        var subscription = new ConnectionMultiplexer.SingleNodeSubscription(CommandFlags.None);
        var firstServer = connection.GetServerEndPoint(server.DefaultEndPoint, ServerProvenance.Configured);
        var first = subscription.EnsureSubscribedToServerAsync(subscriber, channel, CommandFlags.None, true, firstServer);
        try
        {
            await server.FirstSubscribe.Task.WithTimeout(5000);

            // the server can see the subscribe before the client has finished marking that connection as
            // established; an attempt on a connection that is not (yet) established is fair game for takeover,
            // which is not what this test is about
            Assert.True(await Poll.UntilAsync(() => firstServer.IsSubscriberConnected, timeoutMilliseconds: 5000));
            Assert.Equal(1, await subscription.EnsureSubscribedToServerAsync(subscriber, channel, CommandFlags.None, true).WithTimeout(5000));
            Assert.False(subscription.IsConnectedAny());
            Assert.Equal(0, Volatile.Read(ref server.SecondSubscribeCount));
            server.Release.Set();
            await Assert.ThrowsAsync<RedisServerException>(() => first.WithTimeout(5000));
            Assert.True(await Poll.UntilAsync(() => subscription.IsConnectedAny(), timeoutMilliseconds: 5000), "the deferred restore was never re-driven");
            Assert.Equal(1, subscription.GetConnectionCount());
        }
        finally
        {
            server.Release.Set();
        }
    }

    [Fact]
    public async Task ReconnectedSubscriptionUsesTheNewPhysicalConnection()
    {
        using var server = new InProcessTestServer(output);
        var config = server.GetClientConfig();
        config.SharedSubscriptionConnection = false;
        config.HeartbeatInterval = TimeSpan.FromMilliseconds(50);
        await using var connection = await ConnectionMultiplexer.ConnectAsync(config);
        connection.AllowConnect = true;
        var channel = RedisChannel.Literal("connection-identity");
        var delivered = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        await connection.GetSubscriber().SubscribeAsync(channel, (_, value) => delivered.TrySetResult(true));
        Assert.True(connection.TryGetSubscription(channel, out var record));
        var subscription = Assert.IsType<ConnectionMultiplexer.SingleNodeSubscription>(record);
        var endpoint = connection.GetServerEndPoint(server.DefaultEndPoint, ServerProvenance.Configured);
        var previous = endpoint.GetBridge(ConnectionType.Subscription)!.Physical!;
        endpoint.SimulateConnectionFailure(SimulatedFailureType.AllSubscription);
        var deadline = DateTime.UtcNow.AddSeconds(5);
        while (ReferenceEquals(endpoint.GetBridge(ConnectionType.Subscription)!.Physical, previous) || !subscription.IsConnectedAny())
        {
            if (DateTime.UtcNow > deadline) throw new TimeoutException("Subscription did not reconnect.");
            await Task.Delay(20);
        }
        var current = endpoint.GetBridge(ConnectionType.Subscription)!.Physical!;
        Assert.NotSame(previous, current);
        await connection.GetDatabase().PublishAsync(channel, "after-reconnect");
        await delivered.Task.WithTimeout(5000);
    }

    [Fact]
    public async Task UnsubscribeTaskPreservesTheCallersAsyncState()
    {
        using var server = new InProcessTestServer(output);
        await using var connection = await server.ConnectAsync();
        var state = new object();
        var subscriber = connection.GetSubscriber(state);
        var channel = RedisChannel.Literal("async-state");
        await subscriber.SubscribeAsync(channel, (_, _) => { });
        var pending = subscriber.UnsubscribeAsync(channel);
        Assert.Same(state, pending.AsyncState);
        await pending.WithTimeout(5000);
    }

    [Fact]
    public async Task RepeatedSubscriptionReplacementDoesNotCallStoppedHandlers()
    {
        using var server = new InProcessTestServer(output);
        await using var connection = await server.ConnectAsync();
        var subscriber = connection.GetSubscriber();
        var channel = RedisChannel.Literal("repeated-owner");
        int deliveries = 0, stoppedDeliveries = 0;
        for (int i = 0; i < 25; i++)
        {
            int stopped = 0;
            var received = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
            Action<RedisChannel, RedisValue> handler = (_, _) =>
            {
                if (Volatile.Read(ref stopped) != 0) Interlocked.Increment(ref stoppedDeliveries);
                Interlocked.Increment(ref deliveries);
                received.TrySetResult(true);
            };
            await subscriber.SubscribeAsync(channel, handler);
            await subscriber.PublishAsync(channel, i);
            await received.Task.WithTimeout(5000);
            await subscriber.UnsubscribeAsync(channel, handler);
            Volatile.Write(ref stopped, 1);
        }
        Assert.Equal(25, Volatile.Read(ref deliveries));
        Assert.Equal(0, Volatile.Read(ref stoppedDeliveries));
    }

    private sealed class PausedSubscribeServer : InProcessTestServer
    {
        public EndPoint SecondEndPoint { get; }
        public readonly ManualResetEventSlim Release = new();
        public readonly TaskCompletionSource<bool> FirstSubscribe = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public int FirstSubscribeCount, SecondSubscribeCount;
        public bool FailFirstSubscribe;
        public readonly TaskCompletionSource<bool> FirstUnsubscribe = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public PausedSubscribeServer(ITestOutputHelper output) : base(output) => SecondEndPoint = AddEmptyNode();

        public override TypedRedisValue Execute(RedisClient client, in RedisRequest request)
        {
            if ((request.IsString(0, "SUBSCRIBE"u8) || request.IsString(0, "PSUBSCRIBE"u8))
                && request.GetString(1).StartsWith("restoration.", StringComparison.Ordinal))
            {
                TryGetNode(DefaultEndPoint, out var firstNode);
                if (ReferenceEquals(client.Node, firstNode))
                {
                    int attempt = Interlocked.Increment(ref FirstSubscribeCount);
                    FirstSubscribe.TrySetResult(true);
                    if (!Release.Wait(TimeSpan.FromSeconds(10))) throw new TimeoutException("Test subscription barrier timed out.");
                    if (FailFirstSubscribe && attempt == 1) return TypedRedisValue.Error("ERR test: subscribe declined");
                }
                else
                {
                    Interlocked.Increment(ref SecondSubscribeCount);
                }
            }
            var result = base.Execute(client, in request);
            if ((request.IsString(0, "UNSUBSCRIBE"u8) || request.IsString(0, "PUNSUBSCRIBE"u8))
                && request.GetString(1).StartsWith("restoration.", StringComparison.Ordinal))
            {
                TryGetNode(DefaultEndPoint, out var firstNode);
                if (ReferenceEquals(client.Node, firstNode)) FirstUnsubscribe.TrySetResult(true);
            }
            return result;
        }
    }
}
