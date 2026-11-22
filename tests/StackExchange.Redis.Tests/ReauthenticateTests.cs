using System;
using System.Threading;
using System.Threading.Tasks;
using StackExchange.Redis.Server;
using Xunit;

namespace StackExchange.Redis.Tests;

/// <summary>
/// <see cref="ConnectionMultiplexer.ReauthenticateAsync"/> exists for token-based credential rotation (#3372):
/// the dedicated subscription connection cannot be reached via <see cref="IServer"/> commands, so before this
/// API its authentication could only lapse and reconnect - losing any publications in the window. The counts
/// here are asserted from the server's side - how many <c>AUTH</c> commands actually arrived - because the
/// routing (which connections get one) is the thing the API is for.
/// </summary>
public class ReauthenticateTests(ITestOutputHelper log)
{
    private const string Password = "p@55word";

    private sealed class AuthCountingServer(ITestOutputHelper log) : InProcessTestServer(log)
    {
        private int _authCommands, _subscriberAuthCommands;
        public int AuthCommands => Volatile.Read(ref _authCommands);

        /// <summary>
        /// <c>AUTH</c> commands that arrived on a connection with live subscriptions - the connection that
        /// #3372 reports as unreachable. This is the count that proves the routing.
        /// </summary>
        public int SubscriberAuthCommands => Volatile.Read(ref _subscriberAuthCommands);

        public override TypedRedisValue Execute(RedisClient client, in RedisRequest request)
        {
            if (request.Count > 0 && string.Equals(request.GetString(0), "auth", StringComparison.OrdinalIgnoreCase))
            {
                Interlocked.Increment(ref _authCommands);
                if (client.IsSubscriber) Interlocked.Increment(ref _subscriberAuthCommands);
            }
            return base.Execute(client, in request);
        }
    }

    private async Task<(AuthCountingServer Server, ConnectionMultiplexer Connection)> ConnectAsync(
        RedisProtocol protocol,
        bool sharedSubscriptionConnection = false,
        bool withPassword = true)
    {
        var server = new AuthCountingServer(log);
        if (withPassword) server.Password = Password;
        var config = server.GetClientConfig();
        config.Protocol = protocol;
        config.SharedSubscriptionConnection = sharedSubscriptionConnection;
        if (withPassword) config.Password = Password;
        var conn = await ConnectionMultiplexer.ConnectAsync(config);
        return (server, conn);
    }

    /// <summary>
    /// Subscribes and proves delivery, so that the dedicated subscription connection (when there is one) is
    /// fully established before the reauthentication under test.
    /// </summary>
    private static async Task<(RedisChannel Channel, Func<int> Received)> SubscribeAndVerifyAsync(ConnectionMultiplexer conn)
    {
        var channel = RedisChannel.Literal("reauth-channel");
        int received = 0;
        await conn.GetSubscriber().SubscribeAsync(channel, (_, _) => Interlocked.Increment(ref received));
        await AssertDeliversAsync(conn, channel, () => Volatile.Read(ref received));
        return (channel, () => Volatile.Read(ref received));
    }

    /// <summary>
    /// Published repeatedly, because pub/sub is fire-and-forget: a publication sent while the subscription is
    /// still settling is simply dropped, and one publish cannot distinguish that from a broken route.
    /// </summary>
    private static async Task AssertDeliversAsync(ConnectionMultiplexer conn, RedisChannel channel, Func<int> received)
    {
        var before = received();
        var delivered = await Poll.UntilAsync(
            () =>
            {
                if (received() > before) return true;
                conn.GetSubscriber().Publish(channel, "ping", CommandFlags.FireAndForget);
                return received() > before;
            },
            timeoutMilliseconds: 10_000,
            pollMilliseconds: 100);
        Assert.True(delivered, "expected pub/sub delivery");
    }

    [Fact]
    public async Task Resp3DualReauthenticatesBothConnections()
    {
        var (server, conn) = await ConnectAsync(RedisProtocol.Resp3);
        using (server)
        await using (conn)
        {
            var (channel, received) = await SubscribeAndVerifyAsync(conn);

            var before = server.AuthCommands;
            var count = await conn.ReauthenticateAsync();

            Assert.Equal(2, count); // interactive + dedicated subscription
            Assert.Equal(2, server.AuthCommands - before);
            Assert.Equal(1, server.SubscriberAuthCommands); // one landed on the actively-subscribed connection
            await AssertDeliversAsync(conn, channel, received); // the subscription survived
        }
    }

    [Fact]
    public async Task Resp3SharedReauthenticatesTheOneConnection()
    {
        var (server, conn) = await ConnectAsync(RedisProtocol.Resp3, sharedSubscriptionConnection: true);
        using (server)
        await using (conn)
        {
            var (channel, received) = await SubscribeAndVerifyAsync(conn);

            var before = server.AuthCommands;
            var count = await conn.ReauthenticateAsync();

            Assert.Equal(1, count);
            Assert.Equal(1, server.AuthCommands - before);
            await AssertDeliversAsync(conn, channel, received);
        }
    }

    [Fact]
    public async Task Resp2SubscribedSkipsTheSubscriptionConnection()
    {
        // subscriber mode rejects AUTH under RESP2, so that connection must be left alone -
        // it relies on the disconnect/reconnect cycle when its authentication lapses
        var (server, conn) = await ConnectAsync(RedisProtocol.Resp2);
        using (server)
        await using (conn)
        {
            var (channel, received) = await SubscribeAndVerifyAsync(conn);

            var before = server.AuthCommands;
            var count = await conn.ReauthenticateAsync();

            Assert.Equal(1, count); // interactive only
            Assert.Equal(1, server.AuthCommands - before);
            Assert.Equal(0, server.SubscriberAuthCommands); // nothing was sent at the subscriber-mode connection
            await AssertDeliversAsync(conn, channel, received);
        }
    }

    [Fact]
    public async Task Resp2SubscribedWithAllowReconnectRecyclesTheSubscriptionConnection()
    {
        // on a server that *enforces* credential expiry, waiting for the lapse loses publications at a
        // moment of the server's choosing; AllowReconnect trades that for a controlled recycle now
        var (server, conn) = await ConnectAsync(RedisProtocol.Resp2);
        using (server)
        await using (conn)
        {
            var (channel, received) = await SubscribeAndVerifyAsync(conn);

            var before = server.AuthCommands;
            var count = await conn.ReauthenticateAsync(ReauthenticateOptions.AllowReconnect);

            Assert.Equal(2, count); // interactive AUTH + subscription recycle
            // the recycle completes asynchronously: the replacement connection's handshake carries its own
            // AUTH, so the server eventually sees two - one in-place, one from the fresh handshake
            Assert.True(
                await Poll.UntilAsync(() => server.AuthCommands - before >= 2),
                "the recycled subscription connection should re-authenticate via its handshake");
            await AssertDeliversAsync(conn, channel, received); // and the subscription was restored
        }
    }

    // The ad-hoc mirror: existing rotation callers (the Microsoft.Azure.StackExchangeRedis extension does
    // `server.ExecuteAsync("AUTH", user, password)` per server on token refresh) predate ReauthenticateAsync,
    // so an ad-hoc AUTH against an IServer is shadowed onto that server's dedicated subscription connection
    // wherever ReauthenticateAsync itself would AUTH it in place - giving those callers the fix for free.

    [Fact]
    public async Task Resp3AdHocServerAuthMirrorsToSubscriptionConnection()
    {
        var (server, conn) = await ConnectAsync(RedisProtocol.Resp3);
        using (server)
        await using (conn)
        {
            var (channel, received) = await SubscribeAndVerifyAsync(conn);

            var before = server.AuthCommands;
            var result = await conn.GetServer(conn.GetEndPoints()[0]).ExecuteAsync("AUTH", Password);
            Assert.Equal("OK", result.ToString());

            // the shadow is fire-and-forget relative to the caller's command, so poll for it
            Assert.True(
                await Poll.UntilAsync(() => server.AuthCommands - before >= 2),
                "the ad-hoc AUTH should be mirrored to the subscription connection");
            Assert.Equal(2, server.AuthCommands - before);
            Assert.Equal(1, server.SubscriberAuthCommands);
            await AssertDeliversAsync(conn, channel, received); // the subscription survived
        }
    }

    [Fact]
    public async Task Resp2UnsubscribedAdHocServerAuthMirrorsToSubscriptionConnection()
    {
        // a RESP2 subscription connection with no live subscriptions is not in subscriber mode, so it
        // can (and should) be refreshed in place, exactly as ReauthenticateAsync does; established by
        // subscribing then unsubscribing, because the connection only exists once something subscribes
        var (server, conn) = await ConnectAsync(RedisProtocol.Resp2);
        using (server)
        await using (conn)
        {
            var (channel, _) = await SubscribeAndVerifyAsync(conn);
            await conn.GetSubscriber().UnsubscribeAsync(channel);

            var before = server.AuthCommands;
            var result = await conn.GetServer(conn.GetEndPoints()[0]).ExecuteAsync("AUTH", Password);
            Assert.Equal("OK", result.ToString());

            Assert.True(
                await Poll.UntilAsync(() => server.AuthCommands - before >= 2),
                "the ad-hoc AUTH should be mirrored to the idle subscription connection");
            Assert.Equal(2, server.AuthCommands - before);
        }
    }

    [Fact]
    public async Task Resp2SubscribedAdHocServerAuthLeavesTheSubscriberConnectionAlone()
    {
        // subscriber mode rejects AUTH under RESP2: mirroring there would only generate a protocol
        // error on the connection, so the shadow is suppressed - the lapse/reconnect cycle (or an
        // explicit ReauthenticateAsync with AllowReconnect) remains the route for this case
        var (server, conn) = await ConnectAsync(RedisProtocol.Resp2);
        using (server)
        await using (conn)
        {
            var (channel, received) = await SubscribeAndVerifyAsync(conn);

            var before = server.AuthCommands;
            var result = await conn.GetServer(conn.GetEndPoints()[0]).ExecuteAsync("AUTH", Password);
            Assert.Equal("OK", result.ToString());

            // a full pub/sub round-trip gives any (wrongly) issued shadow time to arrive
            await AssertDeliversAsync(conn, channel, received);
            Assert.Equal(1, server.AuthCommands - before);
            Assert.Equal(0, server.SubscriberAuthCommands);
        }
    }

    [Fact]
    public async Task NoCredentialsThrows()
    {
        var (server, conn) = await ConnectAsync(RedisProtocol.Resp3, withPassword: false);
        using (server)
        await using (conn)
        {
            await Assert.ThrowsAsync<InvalidOperationException>(() => conn.ReauthenticateAsync());
        }
    }

    [Fact]
    public async Task RejectedCredentialsFault()
    {
        var (server, conn) = await ConnectAsync(RedisProtocol.Resp3);
        using (server)
        await using (conn)
        {
            await SubscribeAndVerifyAsync(conn);

            server.Password = "rotated-elsewhere"; // the server no longer accepts what we hold
            await Assert.ThrowsAsync<RedisServerException>(() => conn.ReauthenticateAsync());
        }
    }
}
