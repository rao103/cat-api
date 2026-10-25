using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using System.Diagnostics;
namespace CAT.RawFanOut.Observability;

public static class TraceContext
{
    public static ActivityContext Extract(string? traceParent, string? traceState)
    {
        // Bound untrusted metadata. The legacy body TraceId is NOT a W3C parent span.
        if (traceParent is null || traceParent.Length > 512 || traceState?.Length > 512)
            return default;

        return ActivityContext.TryParse(traceParent, traceState, isRemote: true, out var context)
            ? context
            : default;
    }
}
