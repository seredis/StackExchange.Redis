using System;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using StackExchange.Redis.Server;
using Xunit;

namespace StackExchange.Redis.Tests;

// Regression tests for https://github.com/seredis/StackExchange.Redis/issues/3336 - during a
// failover, overlapping subscription-restore paths could each (P)SUBSCRIBE on a different node
// before the first acknowledgement recorded a server, leaving a stray server-side registration
// that delivered duplicate pub/sub messages indefinitely.
[RunPerProtocol]
public class SubscriptionRestoreRaceTests(ITestOutputHelper output)
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

            // while the first subscribe is unacknowledged, a second restore must back off
            var second = subscription.EnsureSubscribedToServerAsync(subscriber, channel, CommandFlags.None, true, secondServer);
            var ping = Message.Create(-1, CommandFlags.None, RedisCommand.PING);
            ping.SetForSubscriptionBridge();
            await connection.ExecuteAsyncImpl(ping, ResultProcessor.ResponseTimer, null, secondServer).WithTimeout(5000);
            Assert.Equal(0, Volatile.Read(ref server.SecondSubscribeCount));

            server.Release.Set();
            await Task.WhenAll(first, second).WithTimeout(5000);
            await WaitForCurrentServerAsync(subscription);
            Assert.Equal(1, Volatile.Read(ref server.FirstSubscribeCount));
            Assert.Equal(0, Volatile.Read(ref server.SecondSubscribeCount));
            Assert.Same(firstServer, subscription.GetAnyCurrentServer());
            Assert.Equal(1, await second); // deferred, but counted as a pending change, not "no change"
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
    public async Task UserSubscribeDoesNotDeferToInFlightRestore(bool pattern)
    {
        using var server = new PausedSubscribeServer(output);
        await using var connection = await server.ConnectAsync();
        var subscriber = connection.DefaultSubscriber;
        var channel = pattern ? RedisChannel.Pattern("restoration.*") : RedisChannel.Literal("restoration.channel");
        var subscription = new ConnectionMultiplexer.SingleNodeSubscription(CommandFlags.None);
        var firstServer = connection.GetServerEndPoint(server.DefaultEndPoint, ServerProvenance.Configured);
        var secondServer = connection.GetServerEndPoint(server.SecondEndPoint, ServerProvenance.Configured);
        var first = subscription.EnsureSubscribedToServerAsync(subscriber, channel, CommandFlags.None, true, firstServer);
        try
        {
            await server.FirstSubscribe.Task.WithTimeout(5000);

            // a *user* subscribe (internalCall: false) must not silently defer to the in-flight
            // restore, which this caller cannot observe failing: it subscribes for itself, and
            // only returns once its own subscribe is acknowledged
            var second = subscription.EnsureSubscribedToServerAsync(subscriber, channel, CommandFlags.None, false, secondServer);
            await second.WithTimeout(5000);
            Assert.Equal(1, Volatile.Read(ref server.SecondSubscribeCount));
            await WaitForCurrentServerAsync(subscription);
            Assert.Same(secondServer, subscription.GetAnyCurrentServer());

            // when the paused restore's ack finally lands it is the stray, and is unsubscribed
            server.Release.Set();
            await first.WithTimeout(5000);
            await server.FirstUnsubscribe.Task.WithTimeout(5000);
            Assert.Same(secondServer, subscription.GetAnyCurrentServer());
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
    public async Task StrayAcknowledgementIsUnsubscribed(bool pattern)
    {
        using var server = new PausedSubscribeServer(output);
        await using var connection = await server.ConnectAsync();
        var subscriber = connection.DefaultSubscriber;
        var channel = pattern ? RedisChannel.Pattern("restoration.*") : RedisChannel.Literal("restoration.channel");
        var subscription = new ConnectionMultiplexer.SingleNodeSubscription(CommandFlags.None);
        var firstServer = connection.GetServerEndPoint(server.DefaultEndPoint, ServerProvenance.Configured);
        var secondServer = connection.GetServerEndPoint(server.SecondEndPoint, ServerProvenance.Configured);

        // confirm the subscription on the first server
        server.Release.Set();
        await subscription.EnsureSubscribedToServerAsync(subscriber, channel, CommandFlags.None, true, firstServer).WithTimeout(5000);
        await WaitForCurrentServerAsync(subscription);
        Assert.Same(firstServer, subscription.GetAnyCurrentServer());

        // simulate an overlapping restore whose ack arrives late: a subscribe pinned to the second
        // server, bypassing the claim; the ack must be rejected and the registration removed
        var stray = subscription.GetSubscriptionMessage(channel, ConnectionMultiplexer.SubscriptionAction.Subscribe, CommandFlags.None, true);
        await connection.ExecuteAsyncImpl(stray, subscription.Processor, null, secondServer).WithTimeout(5000);

        await server.SecondUnsubscribe.Task.WithTimeout(5000);
        Assert.Same(firstServer, subscription.GetAnyCurrentServer());
        Assert.Equal(1, Volatile.Read(ref server.SecondSubscribeCount));
    }

    private static async Task WaitForCurrentServerAsync(ConnectionMultiplexer.SingleNodeSubscription subscription)
    {
        // fire-and-forget variants return before the ack; allow it to land
        for (int i = 0; subscription.GetAnyCurrentServer() is null && i < 50; i++)
        {
            await Task.Delay(100);
        }
    }

    private sealed class PausedSubscribeServer : InProcessTestServer
    {
        public EndPoint SecondEndPoint { get; }
        public readonly ManualResetEventSlim Release = new();
        public readonly TaskCompletionSource<bool> FirstSubscribe = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public int FirstSubscribeCount, SecondSubscribeCount;
        public readonly TaskCompletionSource<bool> FirstUnsubscribe = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public readonly TaskCompletionSource<bool> SecondUnsubscribe = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public PausedSubscribeServer(ITestOutputHelper output) : base(output) => SecondEndPoint = AddEmptyNode();

        public override TypedRedisValue Execute(RedisClient client, in RedisRequest request)
        {
            if ((request.IsString(0, "SUBSCRIBE"u8) || request.IsString(0, "PSUBSCRIBE"u8))
                && request.GetString(1).StartsWith("restoration.", StringComparison.Ordinal))
            {
                TryGetNode(DefaultEndPoint, out var firstNode);
                if (ReferenceEquals(client.Node, firstNode))
                {
                    Interlocked.Increment(ref FirstSubscribeCount);
                    FirstSubscribe.TrySetResult(true);
                    if (!Release.Wait(TimeSpan.FromSeconds(10))) throw new TimeoutException("Test subscription barrier timed out.");
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
                if (ReferenceEquals(client.Node, firstNode))
                {
                    FirstUnsubscribe.TrySetResult(true);
                }
                else
                {
                    SecondUnsubscribe.TrySetResult(true);
                }
            }
            return result;
        }
    }
}
