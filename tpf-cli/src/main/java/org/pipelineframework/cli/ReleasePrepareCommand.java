package org.pipelineframework.cli;

import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.Callable;
import org.pipelineframework.release.producer.DefaultPipelineReleaseProducer;
import org.pipelineframework.release.producer.ReleaseArtifactInput;
import org.pipelineframework.release.producer.ReleaseArtifactInput.Role;
import org.pipelineframework.release.producer.ReleaseProductionRequest;
import picocli.CommandLine.Command;
import picocli.CommandLine.Model.CommandSpec;
import picocli.CommandLine.Option;
import picocli.CommandLine.Spec;

@Command(mixinStandardHelpOptions = true, name = "prepare",
    description = "Prepare a local Release for an HTTP ingress and a whole-pipeline Lambda worker.")
final class ReleasePrepareCommand implements Callable<Integer> {
    @Option(names = "--contract", required = true) private Path contract;
    @Option(names = "--release-version", required = true) private String version;
    @Option(names = "--ingress", required = true) private Path ingress;
    @Option(names = "--worker", required = true) private Path worker;
    @Option(names = "--output", defaultValue = "target/pipeline-release.json") private Path output;
    @Spec private CommandSpec spec;

    @Override
    public Integer call() {
        try {
            Path metadata = contract.toAbsolutePath().normalize().getParent();
            Path descriptor = output.toAbsolutePath().normalize();
            var producer = new DefaultPipelineReleaseProducer();
            var compiledContract = producer.loadContract(contract);
            String carrierId = "compiled-truth";
            var artifacts = producer.materialize(List.of(
                new ReleaseArtifactInput(carrierId, "compiled-truth", metadata,
                    descriptor.resolveSibling("pipeline-release-artifacts").resolve(carrierId + ".zip")
                        .toUri().toASCIIString(), List.of(), List.of()),
                localArtifact("http-ingress", "lambda-zip", ingress, Role.INGRESS),
                localArtifact("work-and-recovery", "lambda-zip", worker, Role.PIPELINE_WORKER)),
                carrierId, metadata, descriptor.getParent());
            producer.write(descriptor, producer.produce(new ReleaseProductionRequest(
                compiledContract, version, carrierId, metadata, artifacts, true)));
            spec.commandLine().getOut().println(descriptor);
            return 0;
        } catch (IllegalArgumentException | IllegalStateException e) {
            return CliSupport.failure(e, spec.commandLine().getErr());
        }
    }

    private static ReleaseArtifactInput localArtifact(String id, String kind, Path file, Role role) {
        return new ReleaseArtifactInput(id, kind, file, file.toAbsolutePath().normalize().toUri().toASCIIString(),
            List.of(), List.of(), role);
    }
}
