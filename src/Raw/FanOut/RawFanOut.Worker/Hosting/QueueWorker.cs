using System;
#nullable enable
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using CAT.RawFanOut.Interfaces;
using CAT.RawFanOut.Models;
using CAT.RawFanOut.Observability;
using CAT.RawFanOut.Operations.DeadLetters;
using CAT.RawFanOut.Operations.Delivery;
using CAT.RawFanOut.Operations.FanOut;
using CAT.RawFanOut.Worker.Configuration;

namespace CAT.RawFanOut.Worker.Hosting;

public sealed class QueueWorker : BackgroundService
{
    private readonly QueueBindings queues;
    private readonly WorkerOptions options;
    private readonly IMessageProcessor processor;
    private readonly DeadLetterProcessor deadLetters;
    private readonly DeliveryHandler handler;
    private readonly DeliveryObserver observer;
    private readonly ITelemetry<QueueWorker> telemetry;

    public QueueWorker(QueueBindings queues, WorkerOptions options, IMessageProcessor processor,
        DeadLetterProcessor deadLetters, DeliveryHandler handler, DeliveryObserver observer, ITelemetry<QueueWorker> telemetry)
    {
        this.queues = queues;
        this.options = options;
        this.processor = processor;
        this.deadLetters = deadLetters;
        this.handler = handler;
        this.observer = observer;
        this.telemetry = telemetry;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        telemetry.Information(
            $"Worker started with {options.ConsumerCount} consumers. DeadLetterConsumer={options.EnableDeadLetterConsumer}");
        try
        {
            var consumers = Enumerable.Range(0, options.ConsumerCount)
                .Select(_ => ConsumeAsync(queues.Raw, false, stoppingToken)).ToList();
            if (options.EnableDeadLetterConsumer)
                consumers.Add(ConsumeAsync(queues.DeadLetter, true, stoppingToken));
            await Task.WhenAll(consumers);
        }
        finally
        {
            telemetry.Information("Worker consumers stopped");
        }
    }

    private async Task ConsumeAsync(IInputQueue queue, bool deadLetter, CancellationToken stoppingToken)
    {
        while (!stoppingToken.IsCancellationRequested)
        {
            QueueDelivery? delivery = null;
            try
            {
                delivery = await queue.ReceiveAsync(stoppingToken);
                if (delivery is null)
                    continue;

                await observer.ObserveAsync(delivery, deadLetter,
                    token => handler.HandleAsync(queue, delivery,
                        processingToken => deadLetter
                            ? deadLetters.ProcessAsync(queues.DeadLetterQueueUrl, delivery, processingToken)
                            : processor.ProcessAsync(delivery.Body, processingToken), token), stoppingToken);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                break;
            }
            catch (Exception error)
            {
                // Delivery failures were logged inside their active correlation scope.
                if (delivery is null)
                    telemetry.Error(
                        $"Queue receive failed. DeadLetter={deadLetter} ErrorType={error.GetType().FullName}", error);

                try
                {
                    await Task.Delay(TimeSpan.FromSeconds(2 + Random.Shared.NextDouble()), stoppingToken);
                }
                catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
                {
                    break;
                }
            }
        }
    }
}
