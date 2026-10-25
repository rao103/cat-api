using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using CAT.RawFanOut.Configuration;
using CAT.RawFanOut.Interfaces;
using CAT.RawFanOut.Models;
using CAT.RawFanOut.Observability;

namespace CAT.RawFanOut.Operations.FanOut;

public sealed class RawFanOutProcessor : IMessageProcessor
{
    private readonly IFanOutPlanner planner;
    private readonly IWorkPublisher publisher;
    private readonly FanOutOptions options;
    private readonly IArtifactDispatchStore artifacts;
    private readonly ITelemetry<RawFanOutProcessor> telemetry;

    public RawFanOutProcessor(
        IFanOutPlanner planner,
        IWorkPublisher publisher,
        FanOutOptions options,
        IArtifactDispatchStore artifacts,
        ITelemetry<RawFanOutProcessor> telemetry)
    {
        this.planner = planner;
        this.publisher = publisher;
        this.options = options;
        this.artifacts = artifacts;
        this.telemetry = telemetry;
    }

    public async Task ProcessAsync(string body, CancellationToken cancellationToken)
    {
        // Correlation properties (ArtifactId, RunId, ...) come from the enclosing delivery or replay scope.
        var artifact = RawEnvelope.Parse(body, options);
        using var scope = telemetry.BeginScope(new Dictionary<string, object?>
        {
            ["OperationId"] = artifact.OperationId,
            ["RunId"] = artifact.RunId,
            ["AttemptId"] = artifact.AttemptId ?? CAT.Messaging.Contracts.RawArtifactConventions.AttemptId(artifact.ExtractionToken),
            ["ArtifactId"] = artifact.ArtifactId,
            ["SourceType"] = artifact.SourceType,
            ["WindfarmId"] = artifact.WindfarmId
        });

        // Reuse a previously frozen plan on redelivery instead of replanning and resaving.
        var durablePlan = await FanOutDiagnostics.MeasureAsync(
            "artifact.load_plan", () => artifacts.LoadPlanAsync(artifact, cancellationToken));
        if (durablePlan is null)
        {
            telemetry.Information($"Planning artifact with {artifact.Paths.Length} files", artifact.RunId);
            var proposedPlan = await FanOutDiagnostics.MeasureAsync(
                "artifact.plan", () => planner.PlanAsync(artifact, cancellationToken));

            // Freeze the complete plan before the first send. Replays reuse committed IDs/bodies.
            durablePlan = await FanOutDiagnostics.MeasureAsync(
                "artifact.persist_plan", () => artifacts.SavePlanAsync(artifact, proposedPlan, cancellationToken));
            telemetry.Information($"Artifact plan persisted with {durablePlan.Count} child batches", artifact.RunId);
        }
        else
        {
            telemetry.Information($"Reusing persisted plan with {durablePlan.Count} batches", artifact.RunId);
        }

        await FanOutDiagnostics.MeasureAsync(
            "artifact.publish", () => publisher.PublishAsync(durablePlan, cancellationToken));
        await FanOutDiagnostics.MeasureAsync(
            "artifact.mark_dispatched", () => artifacts.MarkDispatchedAsync(artifact.ArtifactId, cancellationToken));

        telemetry.Information(
            $"Fan-out completed for {durablePlan.Count} child batches; harmonization is tracked separately",
            artifact.RunId);
    }
}
