using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace StackExchange.Redis;

public partial class ConnectionMultiplexer
{
    /// <summary>
    /// Re-issues <c>AUTH</c> with the currently configured credentials (<see cref="ConfigurationOptions.User"/> and
    /// <see cref="ConfigurationOptions.Password"/>) on every connected server, including dedicated subscription
    /// connections - which are not reachable via <see cref="IServer"/> commands, since those always target the
    /// interactive connection.
    /// </summary>
    /// <remarks>
    /// <para>
    /// This is intended for token-based credential rotation (for example Entra ID tokens on Azure Managed Redis),
    /// where a server retires the authentication of long-lived connections unless they re-authenticate: update
    /// <see cref="ConfigurationOptions.Password"/> (and <see cref="ConfigurationOptions.User"/> where applicable) on
    /// the <see cref="ConfigurationOptions"/> used to create this multiplexer - which also covers any later
    /// reconnects - then call this method to refresh the connections that are already established.
    /// </para>
    /// <para>
    /// A RESP2 connection that has active subscriptions is in subscriber mode, which rejects <c>AUTH</c>; by
    /// default such connections are skipped, and rely on the server disconnecting them when their authentication
    /// lapses (the automatic reconnect then authenticates with the current credentials) - or they can be
    /// proactively recycled via <see cref="ReauthenticateOptions.AllowReconnect"/>. RESP3 subscription
    /// connections, and RESP2 subscription connections with no active subscriptions, are re-authenticated in
    /// place.
    /// </para>
    /// </remarks>
    /// <param name="options">Controls how connections that cannot be re-authenticated in place are treated;
    /// see <see cref="ReauthenticateOptions.AllowReconnect"/>.</param>
    /// <param name="flags">The command flags to use; with <see cref="CommandFlags.FireAndForget"/> the
    /// acknowledgements are not awaited, so authentication failures are not reported.</param>
    /// <returns>The number of connections covered: an <c>AUTH</c> issued, or (with
    /// <see cref="ReauthenticateOptions.AllowReconnect"/>) a recycle initiated - the recycle completes
    /// asynchronously, after this method returns.</returns>
    public async Task<int> ReauthenticateAsync(ReauthenticateOptions options = ReauthenticateOptions.None, CommandFlags flags = CommandFlags.None)
    {
        var config = RawConfig;
        var user = config.User;
        // "" (not null) mirrors the connection handshake, which uses it for 'nopass' logins
        var password = config.Password ?? "";
        if (string.IsNullOrWhiteSpace(user) && string.IsNullOrEmpty(password))
        {
            throw new InvalidOperationException("Cannot reauthenticate: no credentials are configured.");
        }
        CommandMap.AssertAvailable(RedisCommand.AUTH);

        int recycled = 0;
        List<Task<bool>>? pending = null;
        foreach (var server in _serverSnapshot)
        {
            // note the two legs deliberately don't gate on each other: an established subscription
            // connection still deserves the refresh while the interactive one is mid-reconnect, and vice versa
            if (server.IsConnected)
            {
                var interactiveAuth = CreateAuthMessage(user, password, flags, forSubscriptionBridge: false);
                (pending ??= new()).Add(ExecuteAsyncImpl(interactiveAuth, ResultProcessor.DemandOK, state: null, server, defaultValue: false));
            }

            if (!server.SharesSubscriptionConnection()
                && server.GetBridge(ConnectionType.Subscription, create: false) is { IsConnected: true } subscription)
            {
                // in-place AUTH on an actively-subscribed connection is only legal from RESP3; a RESP2
                // connection that has no subscriptions is not yet in subscriber mode, so it still works there
                if (subscription.Physical?.Protocol is >= RedisProtocol.Resp3 || subscription.SubscriptionCount == 0)
                {
                    var subscriptionAuth = CreateAuthMessage(user, password, flags, forSubscriptionBridge: true);
                    (pending ??= new()).Add(ExecuteAsyncImpl(subscriptionAuth, ResultProcessor.DemandOK, state: null, server, defaultValue: false));
                }
                else if ((options & ReauthenticateOptions.AllowReconnect) != 0)
                {
                    // subscriber mode rejects AUTH: recycle instead - the reconnect handshake authenticates
                    // with the current credentials and restores the subscriptions, with new work held in the
                    // backlog meanwhile; this is the same deliberate-drop the MOVING handoff uses
                    if (subscription.RecycleConnection("reauthenticate")) recycled++;
                }
            }
        }

        if (pending is not null)
        {
            await Task.WhenAll(pending).ForAwait();
        }
        return (pending?.Count ?? 0) + recycled;
    }

    private static Message CreateAuthMessage(string? user, string password, CommandFlags flags, bool forSubscriptionBridge)
    {
        var msg = string.IsNullOrWhiteSpace(user)
            ? Message.Create(-1, flags, RedisCommand.AUTH, password.AsRedisValue())
            : Message.Create(-1, flags, RedisCommand.AUTH, user.AsRedisValue(), password.AsRedisValue());
        msg.SetInternalCall();
        if (forSubscriptionBridge)
        {
            msg.SetForSubscriptionBridge();
        }
        return msg;
    }
}
