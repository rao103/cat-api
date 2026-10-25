using System;
using System.Collections.Generic;
using CAT.Messaging.Contracts;
using CAT.RawFanOut.Models;

namespace CAT.RawFanOut.Interfaces;

public interface IRawFanOutStrategy
{
    string SourceType { get; }
    IEnumerable<WorkSelection> BuildWork(FanOutPlanningContext context);
}
