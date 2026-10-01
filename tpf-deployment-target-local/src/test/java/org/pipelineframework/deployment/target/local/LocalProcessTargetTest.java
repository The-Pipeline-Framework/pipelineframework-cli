package org.pipelineframework.deployment.target.local;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

import java.net.ServerSocket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.pipelineframework.deployment.api.DeploymentException;
import org.pipelineframework.deployment.api.DeploymentRequest;
import org.pipelineframework.deployment.api.DeploymentServices;
import org.pipelineframework.deployment.api.DeploymentStatus;
import org.pipelineframework.deployment.release.CredentialResolver;
import org.pipelineframework.deployment.release.LoadedRelease;
import org.pipelineframework.deployment.release.VerifiedRelease;
import org.pipelineframework.orchestrator.PipelineBundleCapabilities;
import org.pipelineframework.orchestrator.release.PipelineContractDescriptor;
import org.pipelineframework.orchestrator.release.PipelineReleaseArtifactDescriptor;
import org.pipelineframework.orchestrator.release.PipelineReleaseDescriptor;
import org.pipelineframework.orchestrator.release.ResolvedPipelineRelease;
import org.pipelineframework.orchestrator.release.ResolvedPipelineReleaseArtifact;

class LocalProcessTargetTest {
    @TempDir Path temporaryDirectory;

    @Test
    void startsMultipleVerifiedNativeUnitsAndReachesActive() throws Exception {
        int firstPort = freePort();
        int secondPort = freePort();
        Path first = script("first", "#!/bin/sh\nexec python3 -m http.server \"$1\" --bind 127.0.0.1\n");
        Path second = script("second", "#!/bin/sh\nexec python3 -m http.server \"$1\" --bind 127.0.0.1\n");
        VerifiedRelease release = verified(List.of(input("first", first), input("second", second)));
        var configuration = new LocalProcessTargetConfiguration(
            temporaryDirectory.resolve("deployments"), "java", List.of(
                unit("first", firstPort, 10), unit("second", secondPort, 10)));
        LocalProcessTarget target = (LocalProcessTarget) new LocalProcessTargetProvider().create(
            configuration, new DeploymentServices(CredentialResolver.NONE));
        try {
            var plan = target.plan(release, new DeploymentRequest("local", "key"));
            var registration = target.register(plan);
            var physical = target.materialize(plan, registration);
            var runtime = target.verifyRuntime(plan, physical);
            var result = target.activate(plan, runtime);
            assertEquals(DeploymentStatus.ACTIVE, result.status());
            assertEquals(2, release.resolved().artifacts().size());
        } finally {
            target.terminateStartedProcesses();
        }
    }

    @Test
    void failedReadinessTerminatesNewProcess() throws Exception {
        Path sleeper = script("sleeper", "#!/bin/sh\necho $$ > sleeper.pid\nexec sleep 30\n");
        VerifiedRelease release = verified(List.of(input("sleeper", sleeper)));
        var configuration = new LocalProcessTargetConfiguration(
            temporaryDirectory.resolve("failed"), "java", List.of(unit("sleeper", freePort(), 1)));
        LocalProcessTarget target = (LocalProcessTarget) new LocalProcessTargetProvider().create(
            configuration, new DeploymentServices(CredentialResolver.NONE));
        var plan = target.plan(release, new DeploymentRequest("local", "key"));
        var registration = target.register(plan);
        var physical = target.materialize(plan, registration);
        org.junit.jupiter.api.Assertions.assertThrows(
            DeploymentException.class, () -> target.verifyRuntime(plan, physical));

        Path pidFile;
        try (var paths = Files.walk(configuration.workspace())) {
            pidFile = paths.filter(path -> path.getFileName().toString().equals("sleeper.pid")).findFirst().orElseThrow();
        }
        long pid = Long.parseLong(Files.readString(pidFile).trim());
        for (int attempt = 0; attempt < 20 && ProcessHandle.of(pid).map(ProcessHandle::isAlive).orElse(false); attempt++) {
            Thread.sleep(50L);
        }
        assertFalse(ProcessHandle.of(pid).map(ProcessHandle::isAlive).orElse(false));
    }

    private LocalProcessTargetConfiguration.Unit unit(String id, int port, int timeout) {
        return new LocalProcessTargetConfiguration.Unit(
            id, List.of(Integer.toString(port)), Map.of(),
            new LocalProcessTargetConfiguration.Readiness("http://127.0.0.1:" + port + "/", timeout));
    }

    private Input input(String id, Path file) {
        return new Input(id, file, new PipelineReleaseArtifactDescriptor(
            id, "native-binary", "file:" + file, "sha256:" + "c".repeat(64), List.of(), List.of()));
    }

    private VerifiedRelease verified(List<Input> inputs) throws Exception {
        List<PipelineReleaseArtifactDescriptor> descriptors = inputs.stream().map(Input::descriptor).toList();
        PipelineReleaseDescriptor descriptor = new PipelineReleaseDescriptor(
            PipelineReleaseDescriptor.CURRENT_SCHEMA_VERSION,
            "local", "sha256:" + "a".repeat(64), "release-1", inputs.getFirst().id(), descriptors);
        PipelineContractDescriptor contract = new PipelineContractDescriptor(
            PipelineContractDescriptor.CURRENT_SCHEMA_VERSION,
            "local", "sha256:" + "a".repeat(64), "a".repeat(64), "COMPUTE", "LOCAL", "app", false, "modular",
            List.of(), new PipelineBundleCapabilities(false, List.of()));
        List<ResolvedPipelineReleaseArtifact> resolved = new ArrayList<>();
        inputs.forEach(input -> resolved.add(new ResolvedPipelineReleaseArtifact(input.descriptor(), input.file())));
        LoadedRelease loaded = new LoadedRelease("{}\n".getBytes(), "sha256:" + "b".repeat(64), descriptor);
        return new VerifiedRelease(loaded, new ResolvedPipelineRelease(
            descriptor, contract, resolved, Files.createDirectories(temporaryDirectory.resolve("truth"))));
    }

    private Path script(String name, String content) throws Exception {
        Path file = temporaryDirectory.resolve("artifacts/" + name);
        Files.createDirectories(file.getParent());
        return Files.writeString(file, content);
    }

    private static int freePort() throws Exception {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }

    private record Input(String id, Path file, PipelineReleaseArtifactDescriptor descriptor) {}
}
