using System;
using System.Threading.Tasks;
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
}
