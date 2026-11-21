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
    /// A RESP2 connection that has active subscriptions is in subscriber mode, which rejects <c>AUTH</c>; such
    /// connections are skipped, and rely on the server disconnecting them when their authentication lapses (the
    /// automatic reconnect then authenticates with the current credentials). RESP3 subscription connections, and
    /// RESP2 subscription connections with no active subscriptions, are re-authenticated in place.
    /// </para>
    /// </remarks>
    /// <param name="flags">The command flags to use; with <see cref="CommandFlags.FireAndForget"/> the
    /// acknowledgements are not awaited, so authentication failures are not reported.</param>
    /// <returns>The number of connections the <c>AUTH</c> was issued to.</returns>
    public async Task<int> ReauthenticateAsync(CommandFlags flags = CommandFlags.None)
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
                && server.GetBridge(ConnectionType.Subscription, create: false) is { IsConnected: true } subscription
                // in-place AUTH on an actively-subscribed connection is only legal from RESP3; a RESP2
                // connection that has no subscriptions is not yet in subscriber mode, so it still works there
                && (subscription.Physical?.Protocol is >= RedisProtocol.Resp3 || subscription.SubscriptionCount == 0))
            {
                var subscriptionAuth = CreateAuthMessage(user, password, flags, forSubscriptionBridge: true);
                (pending ??= new()).Add(ExecuteAsyncImpl(subscriptionAuth, ResultProcessor.DemandOK, state: null, server, defaultValue: false));
            }
        }

        if (pending is null) return 0;
        await Task.WhenAll(pending).ForAwait();
        return pending.Count;
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
