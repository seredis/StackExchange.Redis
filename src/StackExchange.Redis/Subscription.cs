using System;
using System.Buffers;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Net;
using System.Threading;
using System.Threading.Tasks;

namespace StackExchange.Redis;

public partial class ConnectionMultiplexer
{
    /// <summary>
    /// This is the record of a single subscription to a redis server.
    /// It's the singular channel (which may or may not be a pattern), to one or more handlers.
    /// We subscriber to a redis server once (for all messages) and execute 1-many handlers when a message arrives.
    /// </summary>
    internal abstract class Subscription
    {
        private Action<RedisChannel, RedisValue>? _handlers;
        private readonly object _handlersLock = new();
        private ChannelMessageQueue? _queues;

        // Guards this subscription's registration: handlers/queues being added to it, versus it being retired once its
        // last one is removed (at which point it is also removed from the multiplexer, and unsubscribed from the server).
        // Without it, a handler could be added to a subscription that is concurrently being retired; that handler would
        // then never be invoked, since nothing routes messages to a retired subscription (and the server has been told
        // to stop sending them).
        private readonly object _registrationLock = new();
        private volatile bool _isRetired; // one-way: never cleared once set

        /// <summary>
        /// Held while retiring this subscription, see <see cref="TryAdd"/>.
        /// </summary>
        internal object RegistrationLock => _registrationLock;

        // a "false" is only dependable while holding RegistrationLock; a "true" is final, with or without it
        internal bool IsRetired => _isRetired;

        internal void MarkRetired() => _isRetired = true; // only to be called while holding RegistrationLock

        /// <summary>
        /// Adds a handler and/or queue, unless this subscription has been retired (in which case the caller should
        /// look it up again, which yields a fresh one).
        /// </summary>
        internal bool TryAdd(Action<RedisChannel, RedisValue>? handler, ChannelMessageQueue? queue)
        {
            lock (_registrationLock)
            {
                if (_isRetired) return false;
                Add(handler, queue);
                return true;
            }
        }

        public CommandFlags Flags { get; }
        public ResultProcessor.TrackSubscriptionsProcessor Processor { get; }

        internal abstract bool IsConnectedAny();
        internal abstract bool IsConnectedTo(EndPoint endpoint);

        // Called when a (P|S)SUBSCRIBE acknowledgement arrives; returns false when the ack is a
        // stray (we are already confirmed on a different, connected server) and the caller should
        // remove the server-side registration it represents.
        internal abstract bool TryAdoptEndpoint(ServerEndPoint server);

        // conditional clear
        internal abstract bool TryRemoveEndpoint(ServerEndPoint expected);

        internal abstract void RemoveDisconnectedEndpoints();

        // returns the number of changes required
        internal abstract int EnsureSubscribedToServer(
            RedisSubscriber subscriber,
            in RedisChannel channel,
            CommandFlags flags,
            bool internalCall);

        // returns the number of changes required
        internal abstract Task<int> EnsureSubscribedToServerAsync(
            RedisSubscriber subscriber,
            RedisChannel channel,
            CommandFlags flags,
            bool internalCall,
            ServerEndPoint? server = null);

        internal abstract bool UnsubscribeFromServer(
            RedisSubscriber subscriber,
            in RedisChannel channel,
            CommandFlags flags,
            bool internalCall);

        internal abstract Task<bool> UnsubscribeFromServerAsync(
            RedisSubscriber subscriber,
            RedisChannel channel,
            CommandFlags flags,
            object? asyncState,
            bool internalCall);

        internal abstract int GetConnectionCount();

        internal abstract ServerEndPoint? GetAnyCurrentServer();

        public Subscription(CommandFlags flags)
        {
            Flags = flags;
            Processor = new ResultProcessor.TrackSubscriptionsProcessor(this);
        }

        /// <summary>
        /// Gets the configured (P)SUBSCRIBE or (P)UNSUBSCRIBE <see cref="Message"/> for an action.
        /// </summary>
        internal Message GetSubscriptionMessage(
            in RedisChannel channel,
            SubscriptionAction action,
            CommandFlags flags,
            bool internalCall,
            bool stripFireAndForget = false)
        {
            const RedisChannel.RedisChannelOptions OPTIONS_MASK = ~(
                RedisChannel.RedisChannelOptions.KeyRouted | RedisChannel.RedisChannelOptions.IgnoreChannelPrefix);
            var command =
                action switch // note that the Routed flag doesn't impact the message here - just the routing
                {
                    SubscriptionAction.Subscribe => (channel.Options & OPTIONS_MASK) switch
                    {
                        RedisChannel.RedisChannelOptions.None => RedisCommand.SUBSCRIBE,
                        RedisChannel.RedisChannelOptions.MultiNode => RedisCommand.SUBSCRIBE,
                        RedisChannel.RedisChannelOptions.Pattern => RedisCommand.PSUBSCRIBE,
                        RedisChannel.RedisChannelOptions.Pattern | RedisChannel.RedisChannelOptions.MultiNode =>
                            RedisCommand.PSUBSCRIBE,
                        RedisChannel.RedisChannelOptions.Sharded => RedisCommand.SSUBSCRIBE,
                        _ => Unknown(action, channel.Options),
                    },
                    SubscriptionAction.Unsubscribe => (channel.Options & OPTIONS_MASK) switch
                    {
                        RedisChannel.RedisChannelOptions.None => RedisCommand.UNSUBSCRIBE,
                        RedisChannel.RedisChannelOptions.MultiNode => RedisCommand.UNSUBSCRIBE,
                        RedisChannel.RedisChannelOptions.Pattern => RedisCommand.PUNSUBSCRIBE,
                        RedisChannel.RedisChannelOptions.Pattern | RedisChannel.RedisChannelOptions.MultiNode =>
                            RedisCommand.PUNSUBSCRIBE,
                        RedisChannel.RedisChannelOptions.Sharded => RedisCommand.SUNSUBSCRIBE,
                        _ => Unknown(action, channel.Options),
                    },
                    _ => Unknown(action, channel.Options),
                };

            // TODO: Consider flags here - we need to pass Fire and Forget, but don't want to intermingle Primary/Replica
            var effectiveFlags = Flags | flags;
            if (stripFireAndForget)
            {
                // a fire-and-forget message has no result box, so nothing observes its completion;
                // the subscribe-claim logic needs that signal (ack, error reply, connection failure
                // or timeout) to release a pending claim, so the message itself must not be
                // fire-and-forget even when the caller is (the caller then simply doesn't await it)
                effectiveFlags &= ~CommandFlags.FireAndForget;
            }

            var msg = Message.Create(-1, effectiveFlags, command, channel);
            msg.SetForSubscriptionBridge();
            if (internalCall)
            {
                msg.SetInternalCall();
            }

            return msg;
        }

        private RedisCommand Unknown(SubscriptionAction action, RedisChannel.RedisChannelOptions options)
            => throw new ArgumentException(
                $"Unable to determine pub/sub operation for '{action}' against '{options}'");

        public void Add(Action<RedisChannel, RedisValue>? handler, ChannelMessageQueue? queue)
        {
            if (handler != null)
            {
                lock (_handlersLock)
                {
                    _handlers += handler;
                }
            }

            if (queue != null)
            {
                ChannelMessageQueue.Combine(ref _queues, queue);
            }
        }

        public bool Remove(Action<RedisChannel, RedisValue>? handler, ChannelMessageQueue? queue)
        {
            if (handler != null)
            {
                lock (_handlersLock)
                {
                    _handlers -= handler;
                }
            }

            if (queue != null)
            {
                ChannelMessageQueue.Remove(ref _queues, queue);
            }

            return (_handlers == null) & (_queues == null);
        }

        public ICompletable? ForInvoke(in RedisChannel channel, in RedisValue message, out ChannelMessageQueue? queues)
        {
            var handlers = _handlers;
            queues = Volatile.Read(ref _queues);
            return handlers == null ? null : new MessageCompletable(channel, message, handlers);
        }

        internal void MarkCompleted()
        {
            lock (_handlersLock)
            {
                _handlers = null;
            }

            ChannelMessageQueue.MarkAllCompleted(ref _queues);
        }

        internal void GetSubscriberCounts(out int handlers, out int queues)
        {
            queues = ChannelMessageQueue.Count(ref _queues);
            var tmp = _handlers;
            if (tmp == null)
            {
                handlers = 0;
            }
            else if (tmp.IsSingle())
            {
                handlers = 1;
            }
            else
            {
                handlers = 0;
                foreach (var sub in tmp.AsEnumerable()) { handlers++; }
            }
        }
    }

    // used for most subscriptions; routed to a single node
    internal sealed class SingleNodeSubscription(CommandFlags flags) : Subscription(flags)
    {
        internal override bool IsConnectedAny() => _currentServer is { IsSubscriberConnected: true };

        internal override int GetConnectionCount() => IsConnectedAny() ? 1 : 0;

        internal override bool IsConnectedTo(EndPoint endpoint)
        {
            var server = _currentServer;
            return server is { IsSubscriberConnected: true } && server.EndPoint == endpoint;
        }

        internal override bool TryAdoptEndpoint(ServerEndPoint server)
        {
            bool adopted;
            var current = Volatile.Read(ref _currentServer);
            while (true)
            {
                if (current is { IsSubscriberConnected: true } && current != server)
                {
                    // already confirmed on a different live server; the incoming ack is a stray
                    // from an overlapping restore, and should be unsubscribed by the caller
                    adopted = false;
                    break;
                }

                var seen = Interlocked.CompareExchange(ref _currentServer, server, current);
                if (seen == current)
                {
                    adopted = true;
                    break;
                }

                current = seen;
            }

            // an ack from the pending claim's server resolves that claim whether or not we adopted
            // it - but it must be released only *after* the adoption is visible; released first, a
            // competing restore could claim and observe the pre-adoption state (no live server),
            // then wipe the newly-confirmed server and subscribe elsewhere
            var pending = Volatile.Read(ref _pending);
            if (pending is not null && pending.Server == server)
            {
                Interlocked.CompareExchange(ref _pending, null, pending);
            }

            return adopted;
        }

        internal override bool TryRemoveEndpoint(ServerEndPoint expected)
        {
            if (_currentServer == expected)
            {
                _currentServer = null;
                return true;
            }

            return false;
        }

        internal override bool UnsubscribeFromServer(
            RedisSubscriber subscriber,
            in RedisChannel channel,
            CommandFlags flags,
            bool internalCall)
        {
            var server = _currentServer;
            if (server is not null)
            {
                var message = GetSubscriptionMessage(channel, SubscriptionAction.Unsubscribe, flags, internalCall);
                return subscriber.multiplexer.ExecuteSyncImpl(message, Processor, server);
            }

            return true;
        }

        internal override Task<bool> UnsubscribeFromServerAsync(
            RedisSubscriber subscriber,
            RedisChannel channel,
            CommandFlags flags,
            object? asyncState,
            bool internalCall)
        {
            var server = _currentServer;
            if (server is not null)
            {
                var message = GetSubscriptionMessage(channel, SubscriptionAction.Unsubscribe, flags, internalCall);
                return subscriber.multiplexer.ExecuteAsyncImpl(message, Processor, asyncState, server);
            }

            return CompletedTask<bool>.FromResult(true, asyncState);
        }

        private ServerEndPoint? _currentServer;

        // an in-flight subscribe attempt; while one is pending, other restore attempts back off,
        // so that overlapping restores (reconnect + reconfigure racing during a failover) cannot
        // register the same subscription on two different servers (#3336)
        private PendingClaim? _pending;

        internal ServerEndPoint? GetCurrentServer() => Volatile.Read(ref _currentServer);

        internal override ServerEndPoint? GetAnyCurrentServer() => Volatile.Read(ref _currentServer);

        /// <summary>
        /// Evaluates state and if we're not currently connected, clears the server reference.
        /// </summary>
        internal override void RemoveDisconnectedEndpoints()
        {
            var server = _currentServer;
            if (server is { IsSubscriberConnected: false })
            {
                _currentServer = null;
            }

            // note: a pending claim aimed at a server we are no longer connected to is deliberately
            // left in place; it does not obstruct anything (TryClaim takes such a claim over), and
            // clearing it here would discard any restore that deferred to it: its owner's Release
            // would no longer match, so nothing would re-drive that restore
        }

        internal override int EnsureSubscribedToServer(
            RedisSubscriber subscriber,
            in RedisChannel channel,
            CommandFlags flags,
            bool internalCall)
        {
            // a retired subscription is still visible to the restore paths until its unsubscribe has been issued;
            // subscribing it again would leave the server sending messages for a channel the client has forgotten
            if (IsRetired) return 0;

            RemoveIncorrectRouting(subscriber, in channel, flags, internalCall);
            if (IsConnectedAny()) return 0;

            var message = GetSubscriptionMessage(channel, SubscriptionAction.Subscribe, flags, internalCall, stripFireAndForget: true);
            var selected = subscriber.multiplexer.SelectServer(message);
            switch (BeginSubscribeAttempt(subscriber, channel, selected, internalCall, out var claim))
            {
                case SubscribeAttempt.AlreadyConnected: return 0;
                case SubscribeAttempt.Deferred: return 1;
            }

            if (((Flags | flags) & CommandFlags.FireAndForget) != 0)
            {
                ExecuteFireAndObserve(subscriber, message, selected, claim);
            }
            else
            {
                try
                {
                    _ = subscriber.ExecuteSync(message, Processor, selected);
                }
                finally
                {
                    // normally already resolved by the ack; backstop for failures
                    Release(claim);
                }
            }

            return 1;
        }

        private enum SubscribeAttempt
        {
            AlreadyConnected, // a competing attempt confirmed a live server concurrently; nothing to do
            Deferred, // another attempt is in flight and this is an internal restore; it owns the retry
            Begun, // proceed to subscribe (holding the claim, when one was available)
        }

        private SubscribeAttempt BeginSubscribeAttempt(RedisSubscriber subscriber, in RedisChannel channel, ServerEndPoint? selected, bool internalCall, out PendingClaim? claim)
        {
            while (!TryClaim(selected, out claim, out var inFlight))
            {
                if (internalCall)
                {
                    // another subscribe attempt is already in flight; back off - if it is acknowledged,
                    // there is nothing left to do; if it fails, releasing its claim re-drives the restore
                    // on our behalf (the restore paths are event-driven: nothing else would retry it)
                    inFlight.DeferTo(subscriber, channel);
                    if (ReferenceEquals(Volatile.Read(ref _pending), inFlight)) return SubscribeAttempt.Deferred;

                    // it completed while we were registering our interest, so it may not have seen
                    // it; go around again and compete for the claim ourselves
                    continue;
                }

                // a user-initiated subscribe must preserve "acknowledged or thrown" semantics, so it
                // cannot silently defer to the in-flight attempt (which may yet fail, unobserved by
                // this caller); proceed unclaimed - ack-time reconciliation mops up any duplicate
                claim = null;
                break;
            }

            // we're not appropriately connected, so blank the server out for eligible reconnection -
            // but only a stale (disconnected) one: a competing attempt's ack may have installed a
            // live server since the connected check above, and wiping that would orphan a confirmed
            // server-side registration whose ack has already been consumed (#3336)
            var current = Volatile.Read(ref _currentServer);
            if (current is { IsSubscriberConnected: true })
            {
                Release(claim);
                return SubscribeAttempt.AlreadyConnected;
            }

            if (current is not null)
            {
                Interlocked.CompareExchange(ref _currentServer, null, current);
            }

            return SubscribeAttempt.Begun;
        }

        private void ExecuteFireAndObserve(RedisSubscriber subscriber, Message message, ServerEndPoint? server, PendingClaim? claim)
        {
            try
            {
                // fire-and-observe: never block (restore paths run inside result processing),
                // but watch the outcome so a failed attempt releases the claim for retry
                ObserveClaim(subscriber.ExecuteAsync(message, Processor, server), claim);
            }
            catch
            {
                Release(claim);
                throw;
            }
        }

        private bool TryClaim(ServerEndPoint? selected, out PendingClaim? claim, [NotNullWhen(false)] out PendingClaim? inFlight)
        {
            inFlight = null;
            if (selected is null)
            {
                // nothing selectable right now, so there is no server to claim against; the attempt
                // goes unclaimed (ack-time reconciliation still protects against duplicates) - but
                // not while a claimed attempt is in flight on a live connection, which this one
                // would otherwise race onto whichever server the message is eventually routed to
                claim = null;
                var live = Volatile.Read(ref _pending);
                if (live is null || !live.Server.IsSubscriberConnected) return true;

                inFlight = live;
                return false;
            }

            claim = new PendingClaim(this, selected);
            var existing = Volatile.Read(ref _pending);
            while (true)
            {
                // if another subscribe attempt is already in flight, back off; its completion
                // (ack or failure) will resolve the state, retried via the usual restore paths -
                // unless it is aimed at a server we are no longer connected to: that attempt is
                // parked in a backlog that may never drain (or only when a long timeout expires),
                // and nothing re-drives the restores that deferred to it, so the subscription
                // would sit on no server at all; take the claim over instead. If the parked
                // attempt is ever written and acknowledged, ack-time reconciliation mops it up.
                if (existing is not null && existing.Server.IsSubscriberConnected)
                {
                    inFlight = existing;
                    return false;
                }

                var seen = Interlocked.CompareExchange(ref _pending, claim, existing);
                if (ReferenceEquals(seen, existing))
                {
                    // a taken-over claim can no longer release itself (it is not the pending one), so
                    // a restore that deferred to it would be forgotten; we are an attempt for the same
                    // subscription, so that restore is now owed by us instead
                    if (existing is not null && existing.TryTakeDeferred(out var subscriber, out var channel))
                    {
                        claim.DeferTo(subscriber, channel);
                    }

                    return true;
                }

                existing = seen;
            }
        }

        private void Release(PendingClaim? claim)
        {
            if (claim is not null
                && ReferenceEquals(Interlocked.CompareExchange(ref _pending, null, claim), claim)
                && claim.TryTakeDeferred(out var subscriber, out var channel)
                && !IsConnectedAny())
            {
                // this attempt ended without a confirmed server (an ack clears the claim directly, in
                // TryAdoptEndpoint, and never comes through here), and a restore backed off in its
                // favour; that restore is owed, so run it now
                try
                {
                    ObserveClaim(EnsureSubscribedToServerAsync(subscriber, channel, CommandFlags.FireAndForget, internalCall: true), null);
                }
                catch (Exception ex)
                {
                    subscriber.multiplexer.OnInternalError(ex);
                }
            }
        }

        private static void ObserveClaim(Task task, PendingClaim? claim)
            => task.ContinueWith(
                static (t, state) =>
                {
                    _ = t.Exception; // observed; the retry policy is simply "the next restore attempt"
                    (state as PendingClaim)?.Release();
                },
                claim,
                TaskContinuationOptions.ExecuteSynchronously);

        private void RemoveIncorrectRouting(RedisSubscriber subscriber, in RedisChannel channel, CommandFlags flags, bool internalCall)
        {
            // only applies to cluster, when using key-routed channels (sharded, explicit key-routed, or
            // a single-key keyspace notification); is the subscribed server still handling that channel?
            if (channel.IsKeyRouted && _currentServer is { ServerType: ServerType.Cluster } current)
            {
                // if we consider replicas, there can be multiple valid target servers; we can't ask
                // "is this the correct server?", but we can ask "is it suitable?", based on the slot
                if (!subscriber.multiplexer.ServerSelectionStrategy.CanServeSlot(_currentServer, channel))
                {
                    var message = GetSubscriptionMessage(channel, SubscriptionAction.Unsubscribe, flags | CommandFlags.FireAndForget, internalCall);
                    subscriber.multiplexer.ExecuteSyncImpl(message, Processor, current);
                    // pre-emptively disconnect (F+F) - but only the server we just unsubscribed;
                    // a concurrent ack may have installed a different server, which must survive
                    Interlocked.CompareExchange(ref _currentServer, null, current);
                }
            }
        }

        internal override async Task<int> EnsureSubscribedToServerAsync(
            RedisSubscriber subscriber,
            RedisChannel channel,
            CommandFlags flags,
            bool internalCall,
            ServerEndPoint? server = null)
        {
            // a retired subscription is still visible to the restore paths until its unsubscribe has been issued;
            // subscribing it again would leave the server sending messages for a channel the client has forgotten
            if (IsRetired) return 0;

            RemoveIncorrectRouting(subscriber, in channel, flags, internalCall);
            if (IsConnectedAny()) return 0;

            var message = GetSubscriptionMessage(channel, SubscriptionAction.Subscribe, flags, internalCall, stripFireAndForget: true);
            server ??= subscriber.multiplexer.SelectServer(message);
            switch (BeginSubscribeAttempt(subscriber, channel, server, internalCall, out var claim))
            {
                case SubscribeAttempt.AlreadyConnected: return 0;
                case SubscribeAttempt.Deferred: return 1;
            }

            if (((Flags | flags) & CommandFlags.FireAndForget) != 0)
            {
                ExecuteFireAndObserve(subscriber, message, server, claim);
            }
            else
            {
                try
                {
                    await subscriber.ExecuteAsync(message, Processor, server).ForAwait();
                }
                finally
                {
                    // normally already resolved by the ack; backstop for failures
                    Release(claim);
                }
            }

            return 1;
        }

        private sealed class PendingClaim(SingleNodeSubscription owner, ServerEndPoint server)
        {
            private RedisSubscriber? _deferredSubscriber;
            private RedisChannel _deferredChannel;

            public ServerEndPoint Server { get; } = server;

            // records that a restore backed off in favour of this attempt; see Release
            public void DeferTo(RedisSubscriber subscriber, in RedisChannel channel)
            {
                lock (this)
                {
                    _deferredChannel = channel;
                    _deferredSubscriber = subscriber;
                }
            }

            public bool TryTakeDeferred([NotNullWhen(true)] out RedisSubscriber? subscriber, out RedisChannel channel)
            {
                lock (this)
                {
                    subscriber = _deferredSubscriber;
                    channel = _deferredChannel;
                    _deferredSubscriber = null;
                }

                return subscriber is not null;
            }

            public void Release() => owner.Release(this);
        }
    }

    // used for keyspace subscriptions, which are routed to multiple nodes
    internal sealed class MultiNodeSubscription(CommandFlags flags) : Subscription(flags)
    {
        private readonly ConcurrentDictionary<EndPoint, ServerEndPoint> _servers = new();

        internal override bool IsConnectedAny()
        {
            foreach (var server in _servers)
            {
                if (server.Value is { IsSubscriberConnected: true }) return true;
            }

            return false;
        }

        internal override int GetConnectionCount()
        {
            int count = 0;
            foreach (var server in _servers)
            {
                if (server.Value is { IsSubscriberConnected: true }) count++;
            }

            return count;
        }

        internal override bool IsConnectedTo(EndPoint endpoint)
            => _servers.TryGetValue(endpoint, out var server)
               && server.IsSubscriberConnected;

        internal override bool TryAdoptEndpoint(ServerEndPoint server)
        {
            // multi-node subscriptions want every eligible node, so an ack is never a stray
            var ep = server.EndPoint;
            if (!_servers.TryAdd(ep, server))
            {
                _servers[ep] = server;
            }

            return true;
        }

        internal override bool TryRemoveEndpoint(ServerEndPoint expected)
        {
            return _servers.TryRemove(expected.EndPoint, out _);
        }

        internal override ServerEndPoint? GetAnyCurrentServer()
        {
            ServerEndPoint? last = null;
            // prefer actively connected servers, but settle for anything
            foreach (var server in _servers)
            {
                last = server.Value;
                if (last is { IsSubscriberConnected: true })
                {
                    break;
                }
            }

            return last;
        }

        internal override void RemoveDisconnectedEndpoints()
        {
            // This looks more complicated than it is, because of avoiding mutating the collection
            // while iterating; instead, buffer any removals in a scratch buffer, and remove them in a second pass.
            EndPoint[] scratch = [];
            int count = 0;
            foreach (var server in _servers)
            {
                if (!server.Value.IsSubscriberConnected)
                {
                    // flag for removal
                    if (scratch.Length == count) // need to resize the scratch buffer, using the pool
                    {
                        // let the array pool worry about min-sizing etc
                        var newLease = ArrayPool<EndPoint>.Shared.Rent(count + 1);
                        scratch.CopyTo(newLease, 0);
                        ArrayPool<EndPoint>.Shared.Return(scratch);
                        scratch = newLease;
                    }

                    scratch[count++] = server.Key;
                }
            }

            // did we find anything to remove?
            if (count != 0)
            {
                foreach (var ep in scratch.AsSpan(0, count))
                {
                    _servers.TryRemove(ep, out _);
                }
            }

            ArrayPool<EndPoint>.Shared.Return(scratch);
        }

        internal override int EnsureSubscribedToServer(
            RedisSubscriber subscriber,
            in RedisChannel channel,
            CommandFlags flags,
            bool internalCall)
        {
            // a retired subscription is still visible to the restore paths until its unsubscribe has been issued;
            // subscribing it again would leave the server sending messages for a channel the client has forgotten
            if (IsRetired) return 0;

            int delta = 0;
            var muxer = subscriber.multiplexer;
            foreach (var server in muxer.GetServerSnapshot())
            {
                var change = GetSubscriptionChange(server, flags);
                if (change is not null)
                {
                    // make it so
                    var message = GetSubscriptionMessage(channel, change.GetValueOrDefault(), flags, internalCall);
                    subscriber.ExecuteSync(message, Processor, server);
                    delta++;
                }
            }

            return delta;
        }

        private SubscriptionAction? GetSubscriptionChange(ServerEndPoint server, CommandFlags flags)
        {
            // exclude sentinel, and only use replicas if we're explicitly asking for them
            bool useReplica = (Flags & CommandFlags.DemandReplica) != 0;
            bool shouldBeConnected = (server.ServerType != ServerType.Sentinel) & (server.IsReplica == useReplica);
            if (shouldBeConnected == IsConnectedTo(server.EndPoint))
            {
                return null;
            }
            return shouldBeConnected ? SubscriptionAction.Subscribe : SubscriptionAction.Unsubscribe;
        }

        internal override async Task<int> EnsureSubscribedToServerAsync(
            RedisSubscriber subscriber,
            RedisChannel channel,
            CommandFlags flags,
            bool internalCall,
            ServerEndPoint? server = null)
        {
            // a retired subscription is still visible to the restore paths until its unsubscribe has been issued;
            // subscribing it again would leave the server sending messages for a channel the client has forgotten
            if (IsRetired) return 0;

            int delta = 0;
            var muxer = subscriber.multiplexer;
            var snapshot = muxer.GetServerSnaphotMemory();
            var len = snapshot.Length;
            for (int i = 0; i < len; i++)
            {
                var loopServer = snapshot.Span[i]; // spans and async do not mix well
                if (server is null || server == loopServer) // either "all" or "just the one we passed in"
                {
                    var change = GetSubscriptionChange(loopServer, flags);
                    if (change is not null)
                    {
                        // make it so
                        var message = GetSubscriptionMessage(channel, change.GetValueOrDefault(), flags, internalCall);
                        await subscriber.ExecuteAsync(message, Processor, loopServer).ForAwait();
                        delta++;
                    }
                }
            }

            return delta;
        }

        internal override bool UnsubscribeFromServer(
            RedisSubscriber subscriber,
            in RedisChannel channel,
            CommandFlags flags,
            bool internalCall)
        {
            bool any = false;
            foreach (var server in _servers)
            {
                var message = GetSubscriptionMessage(channel, SubscriptionAction.Unsubscribe, flags, internalCall);
                any |= subscriber.ExecuteSync(message, Processor, server.Value);
            }

            return any;
        }

        internal override async Task<bool> UnsubscribeFromServerAsync(
            RedisSubscriber subscriber,
            RedisChannel channel,
            CommandFlags flags,
            object? asyncState,
            bool internalCall)
        {
            // issue all of them before awaiting any, so that they are all issued synchronously (see RegistrationLock)
            var pending = new List<Task<bool>>();
            foreach (var server in _servers)
            {
                var message = GetSubscriptionMessage(channel, SubscriptionAction.Unsubscribe, flags, internalCall);
                pending.Add(subscriber.ExecuteAsync(message, Processor, server.Value));
            }

            bool any = false;
            foreach (var task in pending)
            {
                any |= await task.ForAwait();
            }

            return any;
        }
    }
}
