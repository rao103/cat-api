using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using CAT.RawFanOut.Interfaces;

namespace CAT.RawFanOut.Worker.Hosting;

// Constructed by the composition root. The consumer never constructs cloud adapters.
public sealed record QueueBindings(IInputQueue Raw, IInputQueue DeadLetter, string DeadLetterQueueUrl);
