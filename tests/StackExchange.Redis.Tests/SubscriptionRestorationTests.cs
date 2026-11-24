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

    // An unsubscribe that lands while the subscribe is still unacknowledged has no server to send anything to: the
    // server is only recorded when the acknowledgement arrives. The acknowledgement must then not be adopted by the
    // subscription the client has already forgotten; the registration it confirms is removed instead (#3375).
    [Theory]
    [InlineData(ChannelKind.Literal, false)]
    [InlineData(ChannelKind.Literal, true)]
    [InlineData(ChannelKind.Pattern, false)]
    [InlineData(ChannelKind.Pattern, true)]
    [InlineData(ChannelKind.MultiNode, false)]
    [InlineData(ChannelKind.MultiNode, true)]
    public async Task UnsubscribeDuringAnInFlightSubscribeIsAppliedOnAcknowledgement(ChannelKind kind, bool synchronous)
    {
        using var server = new PausedSubscribeServer(output);
        await using var connection = await server.ConnectAsync();
        var subscriber = connection.GetSubscriber();
        var channel = kind switch
        {
            ChannelKind.Literal => RedisChannel.Literal("restoration.channel"),
            ChannelKind.Pattern => RedisChannel.Pattern("restoration.*"),
            _ => RedisChannel.KeySpacePrefix("restoration.", 0),
        };
        await EstablishSubscriptionConnectionsAsync(connection, server);
        var subscription = connection.GetOrAddSubscription(channel, CommandFlags.None);
        Assert.True(subscription.TryAdd((_, _) => { }, null));
        var firstServer = connection.GetServerEndPoint(server.DefaultEndPoint, ServerProvenance.Configured);
        var pending = subscription.EnsureSubscribedToServerAsync(connection.DefaultSubscriber, channel, CommandFlags.None, false, firstServer);
        try
        {
            await server.FirstSubscribe.Task.WithTimeout(5000);
            if (synchronous)
            {
                subscriber.Unsubscribe(channel);
            }
            else
            {
                await subscriber.UnsubscribeAsync(channel).WithTimeout(5000);
            }
            Assert.False(connection.TryGetSubscription(channel, out _));
            Assert.Equal(0, Volatile.Read(ref server.UnsubscribeCount));

            server.Release.Set();
            await pending.WithTimeout(5000);
            await server.FirstUnsubscribe.Task.WithTimeout(5000);
            Assert.False(subscription.IsConnectedAny());
            Assert.Null(subscription.GetAnyCurrentServer());
        }
        finally { server.Release.Set(); }
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task UnsubscribeAllDuringAnInFlightSubscribeIsAppliedOnAcknowledgement(bool pattern)
    {
        using var server = new PausedSubscribeServer(output);
        await using var connection = await server.ConnectAsync();
        var subscriber = connection.DefaultSubscriber;
        var firstChannel = pattern ? RedisChannel.Pattern("restoration.first.*") : RedisChannel.Literal("restoration.first");
        var secondChannel = pattern ? RedisChannel.Pattern("restoration.second.*") : RedisChannel.Literal("restoration.second");
        await EstablishSubscriptionConnectionsAsync(connection, server);
        var first = connection.GetOrAddSubscription(firstChannel, CommandFlags.None);
        var second = connection.GetOrAddSubscription(secondChannel, CommandFlags.None);
        Assert.True(first.TryAdd((_, _) => { }, null));
        Assert.True(second.TryAdd((_, _) => { }, null));
        var pending = first.EnsureSubscribedToServerAsync(subscriber, firstChannel, CommandFlags.None, false,
            connection.GetServerEndPoint(server.DefaultEndPoint, ServerProvenance.Configured));
        try
        {
            await server.FirstSubscribe.Task.WithTimeout(5000);
            await second.EnsureSubscribedToServerAsync(subscriber, secondChannel, CommandFlags.None, false,
                connection.GetServerEndPoint(server.SecondEndPoint, ServerProvenance.Configured)).WithTimeout(5000);
            Assert.True(await Poll.UntilAsync(() => second.IsConnectedAny(), timeoutMilliseconds: 5000));

            // the acknowledged one is unsubscribed in the usual way; the in-flight one has nowhere to send it yet
            // (UnsubscribeAllAsync only hands back the last unsubscribe it issued, hence the poll)
            await subscriber.UnsubscribeAllAsync().WithTimeout(5000);
            Assert.False(connection.TryGetSubscription(firstChannel, out _));
            Assert.False(connection.TryGetSubscription(secondChannel, out _));
            Assert.True(await Poll.UntilAsync(() => Volatile.Read(ref server.UnsubscribeCount) == 1, timeoutMilliseconds: 5000));
            Assert.False(server.FirstUnsubscribe.Task.IsCompleted);

            server.Release.Set();
            await pending.WithTimeout(5000);
            await server.FirstUnsubscribe.Task.WithTimeout(5000);
            Assert.Equal(2, Volatile.Read(ref server.UnsubscribeCount));
            Assert.False(first.IsConnectedAny());
        }
        finally { server.Release.Set(); }
    }

    // The channel is subscribed afresh before the old subscription's acknowledgement arrives. Removing the old
    // registration must not take the new subscription's with it: on another server it is removed (it would deliver
    // duplicates); on the same server it is the registration the new subscription relies on, and is left alone.
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task LateCleanupDoesNotUnsubscribeTheReplacement(bool sameServer)
    {
        using var server = new PausedSubscribeServer(output);
        await using var connection = await server.ConnectAsync();
        var subscriber = connection.GetSubscriber();
        var channel = RedisChannel.Literal("restoration.owner");
        await EstablishSubscriptionConnectionsAsync(connection, server);
        var old = connection.GetOrAddSubscription(channel, CommandFlags.None);
        Assert.True(old.TryAdd((_, _) => { }, null));
        var first = connection.GetServerEndPoint(server.DefaultEndPoint, ServerProvenance.Configured);
        var second = connection.GetServerEndPoint(server.SecondEndPoint, ServerProvenance.Configured);
        var pending = old.EnsureSubscribedToServerAsync(connection.DefaultSubscriber, channel, CommandFlags.None, false, first);
        try
        {
            await server.FirstSubscribe.Task.WithTimeout(5000);
            await subscriber.UnsubscribeAsync(channel).WithTimeout(5000);
            Assert.False(connection.TryGetSubscription(channel, out _));

            var received = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
            var replacement = connection.AddToSubscription(channel, CommandFlags.None, (_, value) => { if (value == "new-owner") received.TrySetResult(true); }, null);
            Assert.NotSame(old, replacement);
            var replacing = replacement.EnsureSubscribedToServerAsync(connection.DefaultSubscriber, channel, CommandFlags.None, false, sameServer ? first : second);
            if (!sameServer)
            {
                await replacing.WithTimeout(5000);
                Assert.True(await Poll.UntilAsync(() => replacement.IsConnectedAny(), timeoutMilliseconds: 5000));
            }

            server.Release.Set();
            await Task.WhenAll(pending, replacing).WithTimeout(5000);
            if (sameServer)
            {
                // anything the old acknowledgement sent is ahead of this on the subscription connection
                var ping = Message.Create(-1, CommandFlags.None, RedisCommand.PING);
                ping.SetForSubscriptionBridge();
                await connection.ExecuteAsyncImpl(ping, ResultProcessor.ResponseTimer, null, first).WithTimeout(5000);
                Assert.Equal(0, Volatile.Read(ref server.UnsubscribeCount));
            }
            else
            {
                await server.FirstUnsubscribe.Task.WithTimeout(5000);
                Assert.Equal(1, Volatile.Read(ref server.UnsubscribeCount));
            }

            Assert.False(old.IsConnectedAny());
            Assert.True(await Poll.UntilAsync(() => replacement.IsConnectedAny(), timeoutMilliseconds: 5000));
            Assert.Same(sameServer ? first : second, replacement.GetAnyCurrentServer());
            await subscriber.PublishAsync(channel, "new-owner");
            await received.Task.WithTimeout(5000);
        }
        finally { server.Release.Set(); }
    }

    // These tests put a subscription in the multiplexer's map, where the restore that runs as each subscription
    // connection is established can see it, and add subscribes of its own. Subscription connections are opened on
    // first use, so use both here, before anything is in the map; no such restore then runs mid-test.
    private static async Task EstablishSubscriptionConnectionsAsync(ConnectionMultiplexer connection, PausedSubscribeServer server)
    {
        foreach (var endpoint in new[] { server.DefaultEndPoint, server.SecondEndPoint })
        {
            var node = connection.GetServerEndPoint(endpoint, ServerProvenance.Configured);
            var ping = Message.Create(-1, CommandFlags.None, RedisCommand.PING);
            ping.SetForSubscriptionBridge();
            await connection.ExecuteAsyncImpl(ping, ResultProcessor.ResponseTimer, null, node).WithTimeout(5000);
            Assert.True(await Poll.UntilAsync(() => node.IsSubscriberConnected, timeoutMilliseconds: 5000));
        }
    }

    public enum ChannelKind
    {
        Literal,
        Pattern,
        MultiNode,
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
        public int FirstSubscribeCount, SecondSubscribeCount, UnsubscribeCount;
        public bool FailFirstSubscribe;
        public readonly TaskCompletionSource<bool> FirstUnsubscribe = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public PausedSubscribeServer(ITestOutputHelper output) : base(output) => SecondEndPoint = AddEmptyNode();

        public override TypedRedisValue Execute(RedisClient client, in RedisRequest request)
        {
            if ((request.IsString(0, "SUBSCRIBE"u8) || request.IsString(0, "PSUBSCRIBE"u8))
                && request.GetString(1).Contains("restoration."))
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
                && request.GetString(1).Contains("restoration."))
            {
                TryGetNode(DefaultEndPoint, out var firstNode);
                Interlocked.Increment(ref UnsubscribeCount);
                if (ReferenceEquals(client.Node, firstNode)) FirstUnsubscribe.TrySetResult(true);
            }
            return result;
        }
    }
}
