using System.Threading.Tasks;
using Xunit;

namespace StackExchange.Redis.Tests;

/// <summary>
/// <see cref="IConnectionMultiplexer.HashSlot(RedisChannel)"/>, which must agree with how channels are actually routed.
/// </summary>
public class ChannelHashSlotUnitTests(ITestOutputHelper log)
{
    private async Task<ConnectionMultiplexer> ConnectToClusterAsync(InProcessTestServer server, string? channelPrefix = null)
    {
        var config = server.GetClientConfig(defaultOnly: true);
        if (channelPrefix is not null) config.ChannelPrefix = RedisChannel.Literal(channelPrefix);
        return await ConnectionMultiplexer.ConnectAsync(config);
    }

    [Fact]
    public async Task ShardedChannelHasSameSlotAsKeyOfSameName()
    {
        using var server = new InProcessTestServer(log) { ServerType = ServerType.Cluster };
        await using var conn = await ConnectToClusterAsync(server);

        var slot = conn.HashSlot(RedisChannel.Sharded("hash-slot-channel"));

        Assert.NotEqual(ServerSelectionStrategy.NoSlot, slot);
        Assert.Equal(conn.HashSlot((RedisKey)"hash-slot-channel"), slot);
    }

    [Fact]
    public async Task ShardedChannelSlotAccountsForChannelPrefix()
    {
        using var server = new InProcessTestServer(log) { ServerType = ServerType.Cluster };
        await using var conn = await ConnectToClusterAsync(server, channelPrefix: "prefix:");

        var slot = conn.HashSlot(RedisChannel.Sharded("hash-slot-channel"));

        // the prefixed name is what's sent to the server, so that's what the slot is derived from
        Assert.Equal(conn.HashSlot((RedisKey)"prefix:hash-slot-channel"), slot);
        Assert.NotEqual(conn.HashSlot((RedisKey)"hash-slot-channel"), slot);
    }

    [Fact]
    public async Task SingleKeyKeySpaceChannelHasSlotOfItsKey()
    {
        using var server = new InProcessTestServer(log) { ServerType = ServerType.Cluster };
        await using var conn = await ConnectToClusterAsync(server, channelPrefix: "prefix:"); // ignored by keyspace channels

        var channel = RedisChannel.KeySpaceSingleKey("hash-slot-key", 0);

        Assert.Equal(conn.HashSlot((RedisKey)"hash-slot-key"), conn.HashSlot(channel));
        Assert.NotEqual(conn.HashSlot((RedisKey)(string)channel!), conn.HashSlot(channel));
    }

    [Fact]
    public async Task LiteralChannelOnClusterHasSlotOfItsName()
    {
        using var server = new InProcessTestServer(log) { ServerType = ServerType.Cluster };
        await using var conn = await ConnectToClusterAsync(server);

        // regular pub/sub is broadcast cluster-wide by the server, but this client still routes the
        // subscription via the channel's slot
        var slot = conn.HashSlot(RedisChannel.Literal("hash-slot-channel"));

        Assert.NotEqual(ServerSelectionStrategy.NoSlot, slot);
        Assert.Equal(conn.HashSlot((RedisKey)"hash-slot-channel"), slot);
    }

    [Fact]
    public async Task MultiNodeChannelsHaveNoSlot()
    {
        using var server = new InProcessTestServer(log) { ServerType = ServerType.Cluster };
        await using var conn = await ConnectToClusterAsync(server);

        // multi-node channels subscribe on every node without consulting a slot
        Assert.Equal(ServerSelectionStrategy.NoSlot, conn.HashSlot(RedisChannel.KeySpacePattern("hash-slot-*", 0)));
        Assert.Equal(ServerSelectionStrategy.NoSlot, conn.HashSlot(RedisChannel.KeySpacePrefix((RedisKey)"hash-slot:", 0)));
    }

    [Fact]
    public async Task StandaloneHasNoSlot()
    {
        using var server = new InProcessTestServer(log);
        await using var conn = await server.ConnectAsync();

        Assert.Equal(ServerSelectionStrategy.NoSlot, conn.HashSlot(RedisChannel.Sharded("hash-slot-channel")));
    }

    [Fact]
    public async Task StringsAndByteArraysStillBindToKeyOverload()
    {
        // both convert implicitly to RedisKey and RedisChannel; this would fail to compile if the call were ambiguous
        using var server = new InProcessTestServer(log) { ServerType = ServerType.Cluster };
        await using var conn = await ConnectToClusterAsync(server);
        IConnectionMultiplexer viaInterface = conn;

        var expected = conn.HashSlot((RedisKey)"hash-slot-key");

        Assert.Equal(expected, conn.HashSlot("hash-slot-key"));
        Assert.Equal(expected, conn.HashSlot("hash-slot-key"u8.ToArray()));
        Assert.Equal(expected, viaInterface.HashSlot("hash-slot-key"));
    }
}
