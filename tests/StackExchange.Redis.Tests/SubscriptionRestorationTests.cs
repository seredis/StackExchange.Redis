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

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task UnsubscribeWaitsForAnInFlightSubscribe(bool pattern)
    {
        using var server = new PausedSubscribeServer(output);
        await using var connection = await server.ConnectAsync();
        var channel = pattern ? RedisChannel.Pattern("restoration.*") : RedisChannel.Literal("restoration.channel");
        var probe = RedisChannel.Literal("restoration.channel");
        var subscription = new ConnectionMultiplexer.SingleNodeSubscription(CommandFlags.None);
        var firstServer = connection.GetServerEndPoint(server.DefaultEndPoint, ServerProvenance.Configured);
        var pending = subscription.EnsureSubscribedToServerAsync(connection.DefaultSubscriber, channel, CommandFlags.None, true, firstServer);
        try
        {
            await server.FirstSubscribe.Task.WithTimeout(5000);
            var unsubscribe = subscription.UnsubscribeFromServerAsync(connection.DefaultSubscriber, channel, CommandFlags.None, null, false);
            Assert.False(unsubscribe.IsCompleted);
            server.Release.Set();
            await Task.WhenAll(pending, unsubscribe).WithTimeout(5000);
            await server.FirstUnsubscribe.Task.WithTimeout(5000);
            Assert.Null(subscription.GetAnyCurrentServer());
            Assert.Equal(0, await subscription.EnsureSubscribedToServerAsync(connection.DefaultSubscriber, channel, CommandFlags.None, true));
            Assert.Equal(0, await PublishUntilZeroAsync(connection, firstServer, probe));
            Assert.Equal(1, Volatile.Read(ref server.UnsubscribeCount));
        }
        finally { server.Release.Set(); }
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task TimedOutSynchronousUnsubscribeStillDetachesThePendingSubscription(bool pattern)
    {
        using var server = new PausedSubscribeServer(output);
        var config = server.GetClientConfig();
        config.SyncTimeout = 500;
        config.AsyncTimeout = 5000;
        await using var connection = await ConnectionMultiplexer.ConnectAsync(config);
        var subscriber = connection.GetSubscriber();
        var channel = pattern ? RedisChannel.Pattern("restoration.*") : RedisChannel.Literal("restoration.channel");
        var probe = RedisChannel.Literal("restoration.channel");
        var firstServer = connection.GetServerEndPoint(server.DefaultEndPoint, ServerProvenance.Configured);
        var subscription = connection.GetOrAddSubscription(channel, CommandFlags.None);
        subscription.Add((_, _) => { }, null);
        var pending = subscription.EnsureSubscribedToServerAsync(connection.DefaultSubscriber, channel, CommandFlags.None, false, firstServer);
        try
        {
            await server.FirstSubscribe.Task.WithTimeout(5000);
            subscriber.Unsubscribe(channel);
            Assert.False(connection.TryGetSubscription(channel, out _));
            server.Release.Set();
            await pending.WithTimeout(5000);
            await server.FirstUnsubscribe.Task.WithTimeout(5000);
            Assert.Equal(0, await PublishUntilZeroAsync(connection, firstServer, probe));
            Assert.Equal(1, Volatile.Read(ref server.UnsubscribeCount));
        }
        finally { server.Release.Set(); }
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task SyncUnsubscribeDuringInFlightSubscribeThatTimesOutStillUnsubscribes(bool pattern)
    {
        using var server = new PausedSubscribeServer(output);
        var config = server.GetClientConfig();
        config.SyncTimeout = 500;
        config.AsyncTimeout = 500;
        await using var connection = await ConnectionMultiplexer.ConnectAsync(config);
        var subscriber = connection.GetSubscriber();
        var channel = pattern ? RedisChannel.Pattern("restoration.*") : RedisChannel.Literal("restoration.channel");
        var probe = RedisChannel.Literal("restoration.channel");
        var firstServer = connection.GetServerEndPoint(server.DefaultEndPoint, ServerProvenance.Configured);
        var subscription = connection.GetOrAddSubscription(channel, CommandFlags.None);
        subscription.Add((_, _) => { }, null);
        var pending = subscription.EnsureSubscribedToServerAsync(connection.DefaultSubscriber, channel, CommandFlags.None, false, firstServer);
        try
        {
            await server.FirstSubscribe.Task.WithTimeout(5000);

            // Sync unsubscribe while the subscribe is in flight queues a fire-and-forget UNSUBSCRIBE
            // behind it on the pending server, so even if the subscribe then times out (and Message.ComputeResult
            // drops its late ack), the server still ends up unsubscribed.
            subscriber.Unsubscribe(channel);
            Assert.False(connection.TryGetSubscription(channel, out _));
            await Assert.ThrowsAsync<RedisTimeoutException>(() => pending.WithTimeout(5000));

            server.Release.Set();
            await server.FirstUnsubscribe.Task.WithTimeout(5000);
            Assert.Equal(0, await PublishUntilZeroAsync(connection, firstServer, probe));
        }
        finally { server.Release.Set(); }
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task UnsubscribeAllContinuesPastAnInFlightSubscription(bool pattern)
    {
        using var server = new PausedSubscribeServer(output);
        var config = server.GetClientConfig();
        config.SyncTimeout = 500;
        config.AsyncTimeout = 5000;
        await using var connection = await ConnectionMultiplexer.ConnectAsync(config);
        var subscriber = connection.DefaultSubscriber;
        var firstChannel = pattern ? RedisChannel.Pattern("restoration.first.*") : RedisChannel.Literal("restoration.first");
        var secondChannel = pattern ? RedisChannel.Pattern("restoration.second.*") : RedisChannel.Literal("restoration.second");
        var firstProbe = pattern ? RedisChannel.Literal("restoration.first.probe") : firstChannel;
        var secondProbe = pattern ? RedisChannel.Literal("restoration.second.probe") : secondChannel;
        var firstServer = connection.GetServerEndPoint(server.DefaultEndPoint, ServerProvenance.Configured);
        var secondServer = connection.GetServerEndPoint(server.SecondEndPoint, ServerProvenance.Configured);
        var first = connection.GetOrAddSubscription(firstChannel, CommandFlags.None);
        var second = new ConnectionMultiplexer.SingleNodeSubscription(CommandFlags.None);
        var pending = first.EnsureSubscribedToServerAsync(subscriber, firstChannel, CommandFlags.None, false, firstServer);
        try
        {
            await server.FirstSubscribe.Task.WithTimeout(5000);
            await second.EnsureSubscribedToServerAsync(subscriber, secondChannel, CommandFlags.None, false, secondServer).WithTimeout(5000);
            Assert.True(connection.GetSubscriptions().TryAdd(secondChannel, second));
            subscriber.UnsubscribeAll();
            Assert.False(connection.TryGetSubscription(firstChannel, out _));
            Assert.False(connection.TryGetSubscription(secondChannel, out _));
            server.Release.Set();
            await pending.WithTimeout(5000);
            Assert.True(await Poll.UntilAsync(() => Volatile.Read(ref server.UnsubscribeCount) >= 2, timeoutMilliseconds: 5000));
            Assert.Equal(0, await PublishUntilZeroAsync(connection, firstServer, firstProbe));
            Assert.Equal(0, await PublishUntilZeroAsync(connection, secondServer, secondProbe));
        }
        finally { server.Release.Set(); }
    }

    [Fact]
    public async Task LateCleanupDoesNotUnsubscribeTheReplacementOwner()
    {
        using var server = new PausedSubscribeServer(output);
        await using var connection = await server.ConnectAsync();
        var channel = RedisChannel.Literal("restoration.owner");
        var old = connection.GetOrAddSubscription(channel, CommandFlags.None);
        var first = connection.GetServerEndPoint(server.DefaultEndPoint, ServerProvenance.Configured);
        var second = connection.GetServerEndPoint(server.SecondEndPoint, ServerProvenance.Configured);
        var pending = old.EnsureSubscribedToServerAsync(connection.DefaultSubscriber, channel, CommandFlags.None, true, first);
        try
        {
            await server.FirstSubscribe.Task.WithTimeout(5000);
            await connection.GetSubscriber().UnsubscribeAsync(channel, flags: CommandFlags.FireAndForget);
            var replacement = new ConnectionMultiplexer.SingleNodeSubscription(CommandFlags.None);
            var received = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
            replacement.Add((_, value) => { if (value == "new-owner") received.TrySetResult(true); }, null);
            await replacement.EnsureSubscribedToServerAsync(connection.DefaultSubscriber, channel, CommandFlags.None, true, second);
            Assert.True(connection.GetSubscriptions().TryAdd(channel, replacement));
            server.Release.Set();
            await pending.WithTimeout(5000);
            await server.FirstUnsubscribe.Task.WithTimeout(5000);
            Assert.True(replacement.IsConnectedAny());
            Assert.Same(second, replacement.GetAnyCurrentServer());
            Assert.Equal(1, await connection.GetDatabase().PublishAsync(channel, "new-owner"));
            await received.Task.WithTimeout(5000);
        }
        finally { server.Release.Set(); }
    }

    private static async Task<long> PublishUntilZeroAsync(ConnectionMultiplexer connection, ServerEndPoint server, RedisChannel channel)
    {
        var deadline = DateTime.UtcNow + TimeSpan.FromSeconds(5);
        long count;
        do
        {
            count = await connection.ExecuteAsyncImpl(Message.Create(-1, CommandFlags.None, RedisCommand.PUBLISH, channel, "probe"), ResultProcessor.Int64, null, server);
            if (count == 0) break;
            await Task.Delay(50);
        }
        while (DateTime.UtcNow < deadline);
        return count;
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task DeclinedStrayCleanupIsRetried(bool pattern)
    {
        // A live subscription confirmed on firstServer receives a stray subscribe ack from secondServer
        // (the #3336 overlapping-restore scenario). If secondServer declines the cleanup unsubscribe
        // (e.g. BUSY), the heartbeat must retry it until secondServer unsubscribes, leaving only
        // firstServer subscribed (no duplicate delivery).
        using var server = new PausedSubscribeServer(output);
        server.Release.Set();
        var config = server.GetClientConfig();
        config.HeartbeatInterval = TimeSpan.FromMilliseconds(100);
        await using var connection = await ConnectionMultiplexer.ConnectAsync(config);
        var channel = pattern ? RedisChannel.Pattern("restoration.*") : RedisChannel.Literal("restoration.stray");
        var probe = pattern ? RedisChannel.Literal("restoration.stray") : channel;
        var subscription = connection.GetOrAddSubscription(channel, CommandFlags.None);
        int deliveries = 0;
        var delivered = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        subscription.Add((_, value) =>
        {
            if (value == "after-cleanup")
            {
                Interlocked.Increment(ref deliveries);
                delivered.TrySetResult(true);
            }
        }, null);

        var firstServer = connection.GetServerEndPoint(server.DefaultEndPoint, ServerProvenance.Configured);
        var secondServer = connection.GetServerEndPoint(server.SecondEndPoint, ServerProvenance.Configured);

        await subscription.EnsureSubscribedToServerAsync(connection.DefaultSubscriber, channel, CommandFlags.None, false, firstServer).WithTimeout(5000);
        Assert.Same(firstServer, subscription.GetAnyCurrentServer());

        // Make secondServer decline the first 3 cleanup unsubscribes with BUSY, then inject a stray subscribe on it.
        server.RefuseSecondUnsubscribes(3);
        var stray = subscription.GetSubscriptionMessage(channel, ConnectionMultiplexer.SubscriptionAction.Subscribe, CommandFlags.None, internalCall: true);
        await connection.ExecuteAsyncImpl(stray, subscription.Processor, null, secondServer).WithTimeout(5000);

        // Heartbeat retries until secondServer accepts the unsubscribe and only firstServer remains subscribed.
        var deadline = DateTime.UtcNow + TimeSpan.FromSeconds(5);
        long count;
        do
        {
            count = await connection.ExecuteAsyncImpl(Message.Create(-1, CommandFlags.None, RedisCommand.PUBLISH, probe, "probe"), ResultProcessor.Int64, null, firstServer);
            if (count == 1) break;
            await Task.Delay(50);
        }
        while (DateTime.UtcNow < deadline);

        Assert.Equal(1, count);
        Assert.Same(firstServer, subscription.GetAnyCurrentServer());
        Assert.Equal(1, await connection.ExecuteAsyncImpl(Message.Create(-1, CommandFlags.None, RedisCommand.PUBLISH, probe, "after-cleanup"), ResultProcessor.Int64, null, firstServer));
        await delivered.Task.WithTimeout(5000);
        Assert.Equal(1, Volatile.Read(ref deliveries));
    }

    private sealed class PausedSubscribeServer : InProcessTestServer
    {
        public EndPoint SecondEndPoint { get; }
        public readonly ManualResetEventSlim Release = new();
        public readonly TaskCompletionSource<bool> FirstSubscribe = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public int FirstSubscribeCount, SecondSubscribeCount, UnsubscribeCount;
        public bool FailFirstSubscribe;
        private int _secondUnsubscribeRefusals;
        public readonly TaskCompletionSource<bool> FirstUnsubscribe = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public PausedSubscribeServer(ITestOutputHelper output) : base(output) => SecondEndPoint = AddEmptyNode();

        public void RefuseSecondUnsubscribes(int count) => Volatile.Write(ref _secondUnsubscribeRefusals, count);

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
            if ((request.IsString(0, "UNSUBSCRIBE"u8) || request.IsString(0, "PUNSUBSCRIBE"u8))
                && request.GetString(1).StartsWith("restoration.", StringComparison.Ordinal))
            {
                TryGetNode(DefaultEndPoint, out var firstNode);
                if (!ReferenceEquals(client.Node, firstNode) && Interlocked.Decrement(ref _secondUnsubscribeRefusals) >= 0)
                {
                    return TypedRedisValue.Error("BUSY Redis is busy running a script. You can only call SCRIPT KILL or SHUTDOWN NOSAVE.");
                }
            }
            var result = base.Execute(client, in request);
            if ((request.IsString(0, "UNSUBSCRIBE"u8) || request.IsString(0, "PUNSUBSCRIBE"u8))
                && request.GetString(1).StartsWith("restoration.", StringComparison.Ordinal))
            {
                TryGetNode(DefaultEndPoint, out var firstNode);
                Interlocked.Increment(ref UnsubscribeCount);
                if (ReferenceEquals(client.Node, firstNode)) FirstUnsubscribe.TrySetResult(true);
            }
            return result;
        }
    }
}
