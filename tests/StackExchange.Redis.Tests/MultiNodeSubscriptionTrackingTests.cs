using System;
using System.Net;
using System.Threading.Tasks;
using Xunit;

namespace StackExchange.Redis.Tests;

// A multi-node subscription (keyspace notifications) tracks every node it is subscribed on. When any
// subscription connection drops, ServerEndPoint.OnDisconnected asks every subscription to forget its
// *disconnected* nodes; the multi-node flavour had the test inverted, so it forgot the connected nodes
// and kept the dead one. The healthy nodes then looked unsubscribed (IsConnected reported false) and,
// because unsubscribe only walks the tracked nodes, a later unsubscribe never reached them.
public class MultiNodeSubscriptionTrackingTests(ITestOutputHelper output)
{
    [Fact]
    public async Task RemoveDisconnectedEndpointsKeepsConnectedNodesAndDropsDeadOnes()
    {
        using var server = new InProcessTestServer(output);
        await using var connection = await server.ConnectAsync();
        var subscriber = connection.GetSubscriber();
        var channel = RedisChannel.KeySpacePrefix("multinode-tracking:", 0);
        await subscriber.SubscribeAsync(channel, (_, _) => { });

        Assert.True(connection.TryGetSubscription(channel, out var subscription));
        Assert.IsType<ConnectionMultiplexer.MultiNodeSubscription>(subscription);

        // the node is recorded when the acknowledgement is processed, which can trail the awaited subscribe
        for (int i = 0; i < 100 && subscription.GetConnectionCount() == 0; i++)
        {
            await Task.Delay(50);
        }
        Assert.Equal(1, subscription.GetConnectionCount());
        var live = subscription.GetAnyCurrentServer();
        Assert.NotNull(live);
        Assert.True(live.IsSubscriberConnected);

        // a node that is tracked but not connected, as one is after its subscription connection drops
        using var dead = new ServerEndPoint(connection, new IPEndPoint(IPAddress.Loopback, 1), ServerProvenance.Configured);
        Assert.False(dead.IsSubscriberConnected);
        subscription.AddEndpoint(dead);

        // what ServerEndPoint.OnDisconnected runs when any subscription connection drops
        connection.UpdateSubscriptions();

        // the connected node is still tracked...
        Assert.True(subscription.IsConnectedTo(live.EndPoint));
        Assert.Equal(1, subscription.GetConnectionCount());
        Assert.True(subscriber.IsConnected(channel));

        // ...and the dead one is not: with nothing else tracked, "any current server" can only be the live node
        Assert.Same(live, subscription.GetAnyCurrentServer());
        Assert.False(subscription.TryRemoveEndpoint(dead));
    }
}
