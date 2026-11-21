using System;
using Xunit;

namespace StackExchange.Redis.Tests.ResultProcessorUnitTests;

/// <summary>
/// SLOWLOG GET entries have grown over the years: 4 fields originally, client address/name in 4.0,
/// argument count in 8.10, and the estimated CPU time in 8.12. The parser reads positionally and must
/// tolerate every length - reporting <see cref="CommandTrace.CpuTime"/> only when the server sent it.
/// </summary>
public class CommandTraceParse(ITestOutputHelper log) : ResultProcessorUnitTest(log)
{
    private const string FourFields = "*1\r\n*4\r\n:14\r\n:1309448221\r\n:15000\r\n*1\r\n$4\r\nping\r\n";
    private const string SixFields = "*1\r\n*6\r\n:14\r\n:1309448221\r\n:15000\r\n*1\r\n$4\r\nping\r\n$15\r\n127.0.0.1:60525\r\n$6\r\nworker\r\n";
    private const string SevenFields = "*1\r\n*7\r\n:14\r\n:1309448221\r\n:15000\r\n*1\r\n$4\r\nping\r\n$15\r\n127.0.0.1:60525\r\n$6\r\nworker\r\n:1\r\n";
    private const string EightFields = "*1\r\n*8\r\n:14\r\n:1309448221\r\n:15000\r\n*1\r\n$4\r\nping\r\n$15\r\n127.0.0.1:60525\r\n$6\r\nworker\r\n:1\r\n:12345\r\n";
    private const string EightFieldsZeroCpu = "*1\r\n*8\r\n:14\r\n:1309448221\r\n:15000\r\n*1\r\n$4\r\nping\r\n$15\r\n127.0.0.1:60525\r\n$6\r\nworker\r\n:1\r\n:0\r\n";

    [Theory]
    [InlineData(FourFields)] // pre-4.0
    [InlineData(SixFields)] // 4.0+: client address and name
    [InlineData(SevenFields)] // 8.10+: argument count
    [InlineData(EightFields)] // 8.12+: estimated CPU time
    public void CoreFieldsParseAtEveryLength(string resp)
    {
        var result = Execute(resp, CommandTrace.Processor)!;
        var entry = Assert.Single(result);
        Assert.Equal(14, entry.UniqueId);
        Assert.Equal(RedisBase.UnixEpoch.AddSeconds(1309448221), entry.Time);
        Assert.Equal(TimeSpan.FromMilliseconds(15), entry.Duration);
        Assert.Equal("ping", Assert.Single(entry.Arguments));
    }

    [Theory]
    [InlineData(FourFields)]
    [InlineData(SixFields)]
    [InlineData(SevenFields)]
    public void CpuTimeNullWhenNotReported(string resp)
    {
        var entry = Assert.Single(Execute(resp, CommandTrace.Processor)!);
        Assert.Null(entry.CpuTime);
    }

    [Fact]
    public void CpuTimeReportedFrom812()
    {
        var entry = Assert.Single(Execute(EightFields, CommandTrace.Processor)!);
        Assert.Equal(TimeSpan.FromTicks(12345 * 10), entry.CpuTime); // 12,345us
    }

    [Fact]
    public void CpuTimeZeroMeansNotMeasuredButIsStillReported()
    {
        // the server reports 0 for a blocked command, one under 10ms, or off-cpu-tracking off;
        // that is distinct from a pre-8.12 server not reporting the field at all
        var entry = Assert.Single(Execute(EightFieldsZeroCpu, CommandTrace.Processor)!);
        Assert.Equal(TimeSpan.Zero, entry.CpuTime);
    }
}
