package org.pipelineframework.deployment.target.local;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.attribute.PosixFilePermission;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.pipelineframework.deployment.api.DeploymentException;
import org.pipelineframework.deployment.api.DeploymentPlan;
import org.pipelineframework.deployment.api.DeploymentRequest;
import org.pipelineframework.deployment.api.DeploymentResult;
import org.pipelineframework.deployment.api.DeploymentStatus;
import org.pipelineframework.deployment.api.DeploymentTarget;
import org.pipelineframework.deployment.api.PhysicalDeployment;
import org.pipelineframework.deployment.api.ReleaseRegistration;
import org.pipelineframework.deployment.api.RuntimeVerification;
import org.pipelineframework.deployment.api.StageState;
import org.pipelineframework.deployment.release.VerifiedRelease;
import org.pipelineframework.orchestrator.release.ResolvedPipelineReleaseArtifact;

final class LocalProcessTarget implements DeploymentTarget {
    private final LocalProcessTargetConfiguration configuration;
    private final Map<String, ResolvedPipelineReleaseArtifact> selected = new HashMap<>();
    private final List<Process> processes = new ArrayList<>();
    private Path deploymentDirectory;

    LocalProcessTarget(LocalProcessTargetConfiguration configuration) {
        this.configuration = configuration;
    }

    @Override
    public DeploymentPlan plan(VerifiedRelease release, DeploymentRequest request) throws DeploymentException {
        Map<String, ResolvedPipelineReleaseArtifact> available = new HashMap<>();
        release.resolved().artifacts().forEach(artifact -> available.put(artifact.descriptor().artifactId(), artifact));
        for (LocalProcessTargetConfiguration.Unit unit : configuration.units()) {
            ResolvedPipelineReleaseArtifact artifact = available.get(unit.artifactId());
            if (artifact == null) throw targetFailure("Unknown local unit artifactId " + unit.artifactId());
            if (!Set.of("jar", "native-binary").contains(artifact.descriptor().kind())) {
                throw targetFailure("Local process unit " + unit.artifactId() + " must be jar or native-binary");
            }
            if (selected.put(unit.artifactId(), artifact) != null) {
                throw targetFailure("Duplicate local unit artifactId " + unit.artifactId());
            }
        }
        return new DeploymentPlan(release, request, "local-process");
    }

    @Override
    public ReleaseRegistration register(DeploymentPlan plan) {
        return new ReleaseRegistration(plan.release().loaded().descriptorDigest(), StageState.NOT_REQUESTED);
    }

    @Override
    public PhysicalDeployment materialize(DeploymentPlan plan, ReleaseRegistration registration) throws DeploymentException {
        String identity = plan.release().loaded().descriptorDigest().substring("sha256:".length(), "sha256:".length() + 16);
        deploymentDirectory = configuration.workspace().toAbsolutePath().normalize().resolve(identity);
        try {
            Files.createDirectories(deploymentDirectory);
            for (LocalProcessTargetConfiguration.Unit unit : configuration.units()) {
                ResolvedPipelineReleaseArtifact artifact = selected.get(unit.artifactId());
                Path executable = deploymentDirectory.resolve(unit.artifactId());
                Files.copy(artifact.file(), executable, StandardCopyOption.REPLACE_EXISTING);
                List<String> command = new ArrayList<>();
                if (artifact.descriptor().kind().equals("jar")) {
                    command.add(configuration.javaExecutable());
                    command.add("-jar");
                } else {
                    makeExecutable(executable);
                }
                command.add(executable.toString());
                command.addAll(unit.arguments());
                ProcessBuilder builder = new ProcessBuilder(command)
                    .directory(deploymentDirectory.toFile())
                    .redirectOutput(deploymentDirectory.resolve(unit.artifactId() + ".out.log").toFile())
                    .redirectError(deploymentDirectory.resolve(unit.artifactId() + ".err.log").toFile());
                builder.environment().putAll(unit.environment());
                processes.add(builder.start());
            }
            return new PhysicalDeployment(identity, StageState.COMPLETED);
        } catch (IOException | IllegalArgumentException e) {
            terminateStartedProcesses();
            throw new DeploymentException(DeploymentException.FailureClass.TARGET, "Failed to start local deployment", e);
        }
    }

    @Override
    public RuntimeVerification verifyRuntime(DeploymentPlan plan, PhysicalDeployment deployment) throws DeploymentException {
        try {
            for (int index = 0; index < configuration.units().size(); index++) {
                LocalProcessTargetConfiguration.Unit unit = configuration.units().get(index);
                Process process = processes.get(index);
                if (unit.readiness().http().isBlank()) {
                    if (!process.isAlive()) throw new IOException("Process exited before activation: " + unit.artifactId());
                } else {
                    awaitHttp(unit, process);
                }
            }
            return new RuntimeVerification(StageState.COMPLETED, "All local units are ready");
        } catch (IOException | IllegalArgumentException e) {
            terminateStartedProcesses();
            throw new DeploymentException(DeploymentException.FailureClass.RUNTIME, e.getMessage(), e);
        }
    }

    @Override
    public DeploymentResult activate(DeploymentPlan plan, RuntimeVerification verification) {
        return new DeploymentResult(
            deploymentDirectory.getFileName().toString(), DeploymentStatus.ACTIVE,
            StageState.NOT_REQUESTED, StageState.COMPLETED, verification.state(), StageState.COMPLETED,
            "Local deployment is active");
    }

    private static void awaitHttp(LocalProcessTargetConfiguration.Unit unit, Process process) throws IOException {
        HttpClient client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(2)).build();
        Instant deadline = Instant.now().plusSeconds(unit.readiness().timeoutSeconds());
        while (Instant.now().isBefore(deadline)) {
            if (!process.isAlive()) throw new IOException("Process exited before readiness: " + unit.artifactId());
            try {
                HttpResponse<Void> response = client.send(
                    HttpRequest.newBuilder(URI.create(unit.readiness().http())).timeout(Duration.ofSeconds(2)).GET().build(),
                    HttpResponse.BodyHandlers.discarding());
                if (response.statusCode() >= 200 && response.statusCode() < 300) return;
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IOException("Interrupted while checking readiness", e);
            } catch (IOException ignored) {
                // Retry until the explicit readiness deadline.
            }
            try {
                Thread.sleep(200L);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IOException("Interrupted while checking readiness", e);
            }
        }
        throw new IOException("Readiness timed out for " + unit.artifactId());
    }

    void terminateStartedProcesses() {
        processes.forEach(process -> {
            if (process.isAlive()) process.destroyForcibly();
        });
    }

    private static void makeExecutable(Path file) throws IOException {
        try {
            Set<PosixFilePermission> permissions = EnumSet.copyOf(Files.getPosixFilePermissions(file));
            permissions.add(PosixFilePermission.OWNER_EXECUTE);
            Files.setPosixFilePermissions(file, permissions);
        } catch (UnsupportedOperationException ignored) {
            if (!file.toFile().setExecutable(true, true)) throw new IOException("Could not make native artifact executable");
        }
    }

    private static DeploymentException targetFailure(String message) {
        return new DeploymentException(DeploymentException.FailureClass.TARGET, message);
    }
}
