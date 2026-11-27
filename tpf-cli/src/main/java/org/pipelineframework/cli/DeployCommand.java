package org.pipelineframework.cli;

import java.io.PrintWriter;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Callable;
import org.pipelineframework.deployment.api.DeploymentException;
import org.pipelineframework.deployment.api.DeploymentTargetProvider;
import org.pipelineframework.deployment.core.DeploymentConfiguration;
import org.pipelineframework.deployment.core.DeploymentEngine;
import org.pipelineframework.deployment.core.EnvironmentConfiguration;
import org.pipelineframework.deployment.core.EnvironmentCredentialResolver;
import org.pipelineframework.deployment.release.CredentialResolver;
import org.pipelineframework.deployment.release.LoadedRelease;
import org.pipelineframework.deployment.release.ReleaseLoader;
import org.pipelineframework.deployment.target.cloud.CloudArtifactUploader;
import org.pipelineframework.deployment.target.cloud.CloudTargetConfiguration;
import org.pipelineframework.deployment.target.cloud.CloudTargetProvider;
import org.pipelineframework.deployment.target.local.LocalProcessTargetProvider;
import org.pipelineframework.orchestrator.release.PipelineReleaseArtifactDescriptor;
import picocli.CommandLine.Command;
import picocli.CommandLine.Model.CommandSpec;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;
import picocli.CommandLine.Spec;

@Command(mixinStandardHelpOptions = true, name = "deploy", description = "Deploy an already-produced immutable Release.")
final class DeployCommand implements Callable<Integer> {
    @Parameters(index = "0", description = "Named environment from tpf-deploy.yaml") private String environment;
    @Option(names = "--release") private Path release;
    @Option(names = "--config", defaultValue = "tpf-deploy.yaml") private Path config;
    @Option(
        names = "--artifact",
        description = "Upload and verify a local artifact file before deploying (<artifactId>=<path>; repeatable)")
    private List<String> artifacts = new ArrayList<>();
    @Option(names = "--output", defaultValue = "human") private String output;
    @Spec private CommandSpec spec;

    private final CredentialResolver credentials;

    DeployCommand() {
        this(new EnvironmentCredentialResolver());
    }

    DeployCommand(CredentialResolver credentials) {
        this.credentials = Objects.requireNonNull(credentials, "credentials");
    }

    @Override
    public Integer call() {
        PrintWriter out = spec.commandLine().getOut();
        PrintWriter error = spec.commandLine().getErr();
        try {
            CliSupport.requireOutput(output);
            Path descriptor = new ReleasePathLocator().locate(release);
            DeploymentConfiguration loadedConfiguration = CliSupport.configuration(config);
            LoadedRelease releaseIdentity = new ReleaseLoader().load(descriptor);
            if (artifacts != null && !artifacts.isEmpty()) {
                uploadAndVerifyArtifacts(loadedConfiguration, releaseIdentity);
            }
            Map<String, DeploymentTargetProvider<?>> providers = Map.of(
                "local-process", new LocalProcessTargetProvider(),
                "tpf-cloud", new CloudTargetProvider());
            DeploymentEngine engine = new DeploymentEngine(CliSupport.JSON, providers, credentials);
            var result = engine.deploy(environment, descriptor, Path.of(".tpf"), loadedConfiguration);
            Map<String, Object> response = new LinkedHashMap<>();
            response.put("operation", "deploy");
            response.put("environment", environment);
            response.put("status", result.status().name());
            response.put("deploymentId", result.deploymentId());
            response.put("descriptorDigest", releaseIdentity.descriptorDigest());
            response.put("pipelineId", releaseIdentity.descriptor().pipelineId());
            response.put("contractVersion", releaseIdentity.descriptor().contractVersion());
            response.put("releaseVersion", releaseIdentity.descriptor().releaseVersion());
            response.put("registration", result.registration().name());
            response.put("physicalDeployment", result.physicalDeployment().name());
            response.put("runtimeVerification", result.runtimeVerification().name());
            response.put("activation", result.activation().name());
            if (output.equals("json")) CliSupport.json(out, response);
            else out.printf(
                "%s deployment %s for %s/%s/%s (%s)%n",
                result.status(), result.deploymentId(), response.get("pipelineId"), response.get("contractVersion"),
                response.get("releaseVersion"), response.get("descriptorDigest"));
            return 0;
        } catch (Exception e) {
            return CliSupport.failure(e, error);
        }
    }

    private void uploadAndVerifyArtifacts(DeploymentConfiguration loadedConfiguration, LoadedRelease releaseIdentity)
        throws Exception {
        EnvironmentConfiguration envConfig = loadedConfiguration.environments().get(environment);
        if (envConfig == null) {
            throw new IllegalArgumentException("Unknown environment " + environment);
        }
        if (!"tpf-cloud".equals(envConfig.targetType())) {
            throw new IllegalArgumentException(
                "--artifact upload is supported only for tpf-cloud environments; found target type "
                    + envConfig.targetType());
        }
        Map<String, Path> requested = parseArtifactArguments(artifacts);
        Map<String, PipelineReleaseArtifactDescriptor> declaredById = new LinkedHashMap<>();
        for (PipelineReleaseArtifactDescriptor artifact : releaseIdentity.descriptor().artifacts()) {
            declaredById.put(artifact.artifactId(), artifact);
        }
        // Fail fast against the immutable descriptor BEFORE uploading anything.
        for (Map.Entry<String, Path> entry : requested.entrySet()) {
            String artifactId = entry.getKey();
            PipelineReleaseArtifactDescriptor declared = declaredById.get(artifactId);
            if (declared == null) {
                throw new IllegalArgumentException(
                    "Artifact " + artifactId + " is not declared in pipeline-release.json");
            }
            CloudArtifactUploader.FileDigest localDigest = CloudArtifactUploader.digestFile(entry.getValue());
            if (!declared.digest().equals(localDigest.artifactDigest())) {
                throw new DeploymentException(
                    DeploymentException.FailureClass.TARGET,
                    "Local artifact " + artifactId + " digest " + localDigest.artifactDigest()
                        + " does not match immutable pipeline-release.json digest " + declared.digest());
            }
        }
        CloudTargetConfiguration cloudConfig =
            CliSupport.JSON.convertValue(envConfig.targetConfiguration(), CloudTargetConfiguration.class);
        CloudArtifactUploader uploader = new CloudArtifactUploader(cloudConfig, credentials);
        for (Map.Entry<String, Path> entry : requested.entrySet()) {
            String artifactId = entry.getKey();
            PipelineReleaseArtifactDescriptor declared = declaredById.get(artifactId);
            CloudArtifactUploader.UploadResult uploaded = uploader.upload(artifactId, entry.getValue());
            verifyUploadedArtifactMatchesDescriptor(uploaded, declared);
        }
    }

    static Map<String, Path> parseArtifactArguments(List<String> rawArguments) {
        Map<String, Path> parsed = new LinkedHashMap<>();
        for (String raw : rawArguments) {
            int separator = raw == null ? -1 : raw.indexOf('=');
            if (separator <= 0 || separator == raw.length() - 1) {
                throw new IllegalArgumentException(
                    "--artifact must use <artifactId>=<path>: " + raw);
            }
            String artifactId = CloudArtifactUploader.validateArtifactId(raw.substring(0, separator).trim());
            String pathText = raw.substring(separator + 1).trim();
            if (pathText.isEmpty()) {
                throw new IllegalArgumentException("--artifact path is required for " + artifactId);
            }
            if (parsed.put(artifactId, Path.of(pathText)) != null) {
                throw new IllegalArgumentException("Duplicate --artifact entry for " + artifactId);
            }
        }
        return parsed;
    }

    static void verifyUploadedArtifactMatchesDescriptor(
        CloudArtifactUploader.UploadResult uploaded,
        PipelineReleaseArtifactDescriptor declared
    ) throws DeploymentException {
        if (!declared.digest().equals(uploaded.artifactDigest())) {
            throw new DeploymentException(
                DeploymentException.FailureClass.TARGET,
                "Uploaded artifact " + declared.artifactId() + " digest " + uploaded.artifactDigest()
                    + " does not match immutable pipeline-release.json digest " + declared.digest());
        }
        if (declared.uri() != null && declared.uri().startsWith("s3://")) {
            if (!s3UriMatches(uploaded.artifactUri(), declared.uri())) {
                throw new DeploymentException(
                    DeploymentException.FailureClass.TARGET,
                    "Uploaded artifact " + declared.artifactId() + " URI " + uploaded.artifactUri()
                        + " does not match immutable pipeline-release.json URI " + declared.uri());
            }
        }
    }

    private static boolean s3UriMatches(String uploadedUri, String declaredUri) {
        if (declaredUri.equals(uploadedUri)) {
            return true;
        }
        // When the descriptor omits ?versionId=..., accept a version-pinned S3 URI for the exact same bucket/key.
        if (!declaredUri.contains("?") && uploadedUri != null) {
            int queryIndex = uploadedUri.indexOf('?');
            if (queryIndex > 0 && declaredUri.equals(uploadedUri.substring(0, queryIndex))) {
                return true;
            }
        }
        return false;
    }
}
