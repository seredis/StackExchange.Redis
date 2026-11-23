using System;
using System.Diagnostics.CodeAnalysis;
using RESPite;
using RESPite.Messages;

namespace StackExchange.Redis
{
    /// <summary>
    /// Represents the information known about long-running commands.
    /// </summary>
    public sealed class CommandTrace
    {
        internal static readonly ResultProcessor<CommandTrace[]> Processor = new CommandTraceProcessor();

        internal CommandTrace(long uniqueId, long time, long duration, RedisValue[] arguments, long cpuTimeMicroseconds = -1)
        {
            UniqueId = uniqueId;
            Time = RedisBase.UnixEpoch.AddSeconds(time);
            // duration = The amount of time needed for its execution, in microseconds.
            // A tick is equal to 100 nanoseconds, or one ten-millionth of a second.
            // So 1 microsecond = 10 ticks
            Duration = TimeSpan.FromTicks(duration * 10);
            Arguments = arguments;
#pragma warning disable SER011 // assigning the experimental property
            CpuTime = cpuTimeMicroseconds < 0 ? null : TimeSpan.FromTicks(cpuTimeMicroseconds * 10);
#pragma warning restore SER011
        }

        /// <summary>
        /// The array composing the arguments of the command.
        /// </summary>
        public RedisValue[] Arguments { get; }

        /// <summary>
        /// The amount of time needed for its execution.
        /// </summary>
        public TimeSpan Duration { get; }

        /// <summary>
        /// The time at which the logged command was processed.
        /// </summary>
        public DateTime Time { get; }

        /// <summary>
        /// An upper-bound estimate of the executing thread's CPU time (user and system) for the command:
        /// the duration minus the off-CPU time that provably happened during it. This distinguishes a command
        /// that ran slowly from one that was starved of CPU. Reported by Redis 8.12 and later;
        /// <see langword="null"/> when the server did not report it. The server reports
        /// <see cref="TimeSpan.Zero"/> when the time was not measured: a command that was blocked, one
        /// shorter than 10 milliseconds, or <c>off-cpu-tracking</c> disabled.
        /// </summary>
        [Experimental(Experiments.Server_8_12, UrlFormat = Experiments.UrlFormat)]
        public TimeSpan? CpuTime { get; }

        /// <summary>
        /// A unique progressive identifier for every slow log entry.
        /// </summary>
        /// <remarks>The entry's unique ID can be used in order to avoid processing slow log entries multiple times (for instance you may have a script sending you an email alert for every new slow log entry). The ID is never reset in the course of the Redis server execution, only a server restart will reset it.</remarks>
        public long UniqueId { get; }

        /// <summary>
        /// Deduces a link to the redis documentation about the specified command.
        /// </summary>
        public string? GetHelpUrl()
        {
            if (Arguments == null || Arguments.Length == 0) return null;

            const string BaseUrl = "https://redis.io/commands/";

            string encoded0 = Uri.EscapeDataString(((string)Arguments[0]!).ToLowerInvariant());

            if (Arguments.Length > 1)
            {
                switch (encoded0)
                {
                    case "script":
                    case "client":
                    case "cluster":
                    case "config":
                    case "debug":
                    case "pubsub":
                        string encoded1 = Uri.EscapeDataString(((string)Arguments[1]!).ToLowerInvariant());
                        return BaseUrl + encoded0 + "-" + encoded1;
                }
            }
            return BaseUrl + encoded0;
        }

        private sealed class CommandTraceProcessor : ResultProcessor<CommandTrace[]>
        {
            protected override bool SetResultCore(PhysicalConnection connection, Message message, ref RespReader reader)
            {
                // see: SLOWLOG GET
                if (reader.IsAggregate)
                {
                    var arr = reader.ReadPastArray(ParseOne, scalar: false)!;
                    if (arr.AnyNull()) return false;

                    SetResult(message, arr);
                    return true;
                }

                return false;

                static CommandTrace ParseOne(ref RespReader reader)
                {
                    CommandTrace result = null!;
                    if (reader.IsAggregate)
                    {
                        long uniqueId = 0, time = 0, duration = 0;
                        if (reader.TryMoveNext() && reader.IsScalar && reader.TryReadInt64(out uniqueId)
                            && reader.TryMoveNext() && reader.IsScalar && reader.TryReadInt64(out time)
                            && reader.TryMoveNext() && reader.IsScalar && reader.TryReadInt64(out duration)
                            && reader.TryMoveNext() && reader.IsAggregate)
                        {
                            var values = reader.ReadPastRedisValues() ?? [];
                            // positional trailing fields: 5 client address (4.0+), 6 client name (4.0+),
                            // 7 argument count (8.10+), 8 estimated CPU time in microseconds (8.12+);
                            // negative means "not reported", so an older server's shorter entry maps to null
                            long cpuTime = -1;
                            if (reader.TryMoveNext() && reader.TryMoveNext() && reader.TryMoveNext()
                                && reader.TryMoveNext() && reader.IsScalar && reader.TryReadInt64(out var micros))
                            {
                                cpuTime = micros;
                            }
                            result = new CommandTrace(uniqueId, time, duration, values, cpuTime);
                        }
                    }
                    return result;
                }
            }
        }
    }
}
