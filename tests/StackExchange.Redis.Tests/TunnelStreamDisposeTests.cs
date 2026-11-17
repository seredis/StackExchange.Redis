using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;
using StackExchange.Redis.Configuration;
using Xunit;

namespace StackExchange.Redis.Tests;

public class TunnelStreamDisposeTests(ITestOutputHelper output)
{
    /// <summary>
    /// A socketless tunnel (<see cref="Tunnel.GetSocketConnectEndpointAsync"/> returning null, with
    /// <see cref="Tunnel.BeforeAuthenticateAsync"/> supplying the stream) has no socket whose disposal
    /// tears the connection down: the stream is the connection's only handle, so
    /// <c>PhysicalConnection.Shutdown</c> must dispose it, or the tunnel's underlying transport leaks.
    /// </summary>
    [Fact]
    public async Task SocketlessTunnelStreamIsDisposedOnShutdown()
    {
        using var server = new InProcessTestServer(output);
        var tunnel = new TrackingTunnel(server.Tunnel!);
        var config = server.GetClientConfig(withPubSub: false, defaultOnly: true);
        config.Tunnel = tunnel;

        var conn = await ConnectionMultiplexer.ConnectAsync(config);
        await conn.GetDatabase().PingAsync();
        TrackingStream[] streams;
        lock (tunnel.Streams)
        {
            streams = tunnel.Streams.ToArray();
        }
        Assert.NotEmpty(streams);

        await conn.DisposeAsync();

        foreach (var stream in streams)
        {
            await stream.Disposed.Task.WaitAsync(TimeSpan.FromSeconds(5));
        }
    }

    /// <summary>Delegates to the in-process server's own (socketless) tunnel, wrapping each stream it
    /// hands out so the test can observe disposal.</summary>
    private sealed class TrackingTunnel(Tunnel inner) : Tunnel
    {
        public List<TrackingStream> Streams { get; } = new();

        public override ValueTask<EndPoint?> GetSocketConnectEndpointAsync(EndPoint endpoint, CancellationToken cancellationToken)
            => inner.GetSocketConnectEndpointAsync(endpoint, cancellationToken);

        public override async ValueTask<Stream?> BeforeAuthenticateAsync(EndPoint endpoint, ConnectionType connectionType, Socket? socket, CancellationToken cancellationToken)
        {
            var stream = await inner.BeforeAuthenticateAsync(endpoint, connectionType, socket, cancellationToken);
            if (stream is null)
            {
                return null;
            }
            var tracked = new TrackingStream(stream);
            lock (Streams)
            {
                Streams.Add(tracked);
            }
            return tracked;
        }
    }

    private sealed class TrackingStream(Stream inner) : Stream
    {
        public TaskCompletionSource<bool> Disposed { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public override bool CanRead => inner.CanRead;
        public override bool CanWrite => inner.CanWrite;
        public override bool CanSeek => false;
        public override long Length => throw new NotSupportedException();
        public override long Position
        {
            get => throw new NotSupportedException();
            set => throw new NotSupportedException();
        }

        public override int Read(byte[] buffer, int offset, int count) => inner.Read(buffer, offset, count);
        public override Task<int> ReadAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken) => inner.ReadAsync(buffer, offset, count, cancellationToken);
        public override void Write(byte[] buffer, int offset, int count) => inner.Write(buffer, offset, count);
        public override Task WriteAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken) => inner.WriteAsync(buffer, offset, count, cancellationToken);
        public override void Flush() => inner.Flush();
        public override Task FlushAsync(CancellationToken cancellationToken) => inner.FlushAsync(cancellationToken);
        public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();
        public override void SetLength(long value) => throw new NotSupportedException();
#if !NETFRAMEWORK
        public override int Read(Span<byte> buffer) => inner.Read(buffer);
        public override ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken = default) => inner.ReadAsync(buffer, cancellationToken);
        public override void Write(ReadOnlySpan<byte> buffer) => inner.Write(buffer);
        public override ValueTask WriteAsync(ReadOnlyMemory<byte> buffer, CancellationToken cancellationToken = default) => inner.WriteAsync(buffer, cancellationToken);

        public override async ValueTask DisposeAsync()
        {
            await inner.DisposeAsync().ConfigureAwait(false);
            Disposed.TrySetResult(true);
        }
#endif

        protected override void Dispose(bool disposing)
        {
            if (disposing)
            {
                inner.Dispose();
                Disposed.TrySetResult(true);
            }
            base.Dispose(disposing);
        }
    }
}
