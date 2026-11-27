using System;

namespace StackExchange.Redis;

/// <summary>
/// Controls how <see cref="IConnectionMultiplexer.ReauthenticateAsync"/> treats connections that cannot be
/// re-authenticated in place.
/// </summary>
[Flags]
public enum ReauthenticateOptions
{
    /// <summary>
    /// Only re-authenticate connections where an in-place <c>AUTH</c> is possible; a RESP2 connection with
    /// active subscriptions is in subscriber mode (which rejects <c>AUTH</c>) and is left untouched, relying
    /// on the server disconnecting it when its authentication lapses.
    /// </summary>
    None = 0,

    /// <summary>
    /// When an in-place <c>AUTH</c> is not possible (a RESP2 connection in subscriber mode), proactively
    /// recycle that connection instead: it reconnects immediately, authenticating with the current credentials
    /// and restoring its subscriptions. This trades a brief, immediate interruption - publications arriving
    /// during the reconnect are lost, as in any pub/sub reconnect - for control over <em>when</em> the
    /// interruption happens, rather than waiting for a server that enforces credential expiry to drop the
    /// connection at a time of its choosing. On servers that do not enforce credential expiry on established
    /// connections (which includes vanilla Redis password rotation), this costs an interruption for no benefit,
    /// which is why it is not the default.
    /// </summary>
    AllowReconnect = 1,
}
