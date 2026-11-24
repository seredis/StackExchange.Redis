using System;
using System.Threading;
using System.Threading.Tasks;
using StackExchange.Redis.Server;
using Xunit;

namespace StackExchange.Redis.Tests;

public class SubscriptionRegistrationRaceUnitTests(ITestOutputHelper output)
{
    /// <summary>
    /// Removing a channel's last handler retires its subscription (locally, and on the server); a handler added to the
    /// same channel concurrently must end up on a live subscription, rather than on the retired one - which nothing
    /// routes messages to anymore, and which the server is unsubscribed from.
    /// </summary>
    [Fact]
    public async Task SubscribeRacingLastUnsubscribeStillReceivesMessages()
    {
        using var server = new InProcessTestServer(output);
        await using var conn = await ConnectionMultiplexer.ConnectAsync(server.GetClientConfig());
        var subscriber = conn.GetSubscriber();

        // the race is narrow, so it takes a number of attempts to provoke it; each uses a fresh channel
        const int Attempts = 300;
        var missed = 0;
        for (var i = 0; i < Attempts; i++)
        {
            var channel = RedisChannel.Literal($"{nameof(SubscribeRacingLastUnsubscribeStillReceivesMessages)}:{i}");
            Action<RedisChannel, RedisValue> leaving = static (_, _) => { };
            var received = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
            Action<RedisChannel, RedisValue> joining = (_, _) => received.TrySetResult(true);

            await subscriber.SubscribeAsync(channel, leaving);

            var start = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
            var unsubscribe = Task.Run(async () =>
            {
                await start.Task;
                await subscriber.UnsubscribeAsync(channel, leaving);
            });
            var subscribe = Task.Run(async () =>
            {
                await start.Task;
                await subscriber.SubscribeAsync(channel, joining);
            });
            start.SetResult(true);
            await Task.WhenAll(unsubscribe, subscribe);

            await subscriber.PublishAsync(channel, "hello");
            if (await Task.WhenAny(received.Task, Task.Delay(TimeSpan.FromSeconds(2))) != received.Task)
            {
                missed++;
            }

            await subscriber.UnsubscribeAsync(channel, joining);
        }

        Assert.Equal(0, missed);
    }

    /// <summary>
    /// A retired subscription stays in the multiplexer until its unsubscribe has been issued, so the restore paths
    /// (which walk the multiplexer's subscriptions after a reconnect or reconfigure) can still see it; they must not
    /// subscribe it again, since nothing would ever unsubscribe it: the client is about to forget it.
    /// </summary>
    [Theory]
    [InlineData(SubscriptionKind.Literal, false)]
    [InlineData(SubscriptionKind.Literal, true)]
    [InlineData(SubscriptionKind.Pattern, false)]
    [InlineData(SubscriptionKind.Pattern, true)]
    [InlineData(SubscriptionKind.MultiNode, false)]
    [InlineData(SubscriptionKind.MultiNode, true)]
    public async Task RestoreDoesNotResubscribeARetiredSubscription(SubscriptionKind kind, bool async)
    {
        using var server = new SubscribeCountingServer(output);
        await using var conn = await ConnectionMultiplexer.ConnectAsync(server.GetClientConfig());
        var channel = kind switch
        {
            SubscriptionKind.Literal => RedisChannel.Literal(SubscribeCountingServer.Marker),
            SubscriptionKind.Pattern => RedisChannel.Pattern(SubscribeCountingServer.Marker + "*"),
            _ => RedisChannel.KeySpacePrefix(SubscribeCountingServer.Marker, 0),
        };

        // a subscription that has been retired, but not yet removed: the state a last-handler unsubscribe is in
        // for as long as it takes to issue the unsubscribe; it was never subscribed on a server, as is the case
        // once a disconnect has been noticed
        var sub = conn.GetOrAddSubscription(channel, CommandFlags.None);
        lock (sub.RegistrationLock)
        {
            sub.MarkRetired();
        }
        Assert.False(sub.IsConnectedAny());

        if (async)
        {
            Assert.Equal(0, await sub.EnsureSubscribedToServerAsync(conn.DefaultSubscriber, channel, CommandFlags.None, true));
        }
        else
        {
            // what a reconnect or reconfigure runs
            Assert.Equal(0, conn.EnsureSubscriptions(CommandFlags.FireAndForget));
        }

        // anything the restore sent is ahead of this on the subscription connection
        await conn.GetSubscriber().PingAsync();
        Assert.Equal(0, Volatile.Read(ref server.SubscribeCount));
        Assert.False(sub.IsConnectedAny());
    }

    public enum SubscriptionKind
    {
        Literal,
        Pattern,
        MultiNode,
    }

    private sealed class SubscribeCountingServer(ITestOutputHelper output) : InProcessTestServer(output)
    {
        public const string Marker = "retired-restore";
        public int SubscribeCount;

        public override TypedRedisValue Execute(RedisClient client, in RedisRequest request)
        {
            if ((request.IsString(0, "SUBSCRIBE"u8) || request.IsString(0, "PSUBSCRIBE"u8))
                && request.GetString(1).Contains(Marker))
            {
                Interlocked.Increment(ref SubscribeCount);
            }
            return base.Execute(client, in request);
        }
    }
}
