package org.pipelineframework.cli;

import java.io.PrintWriter;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Callable;
import org.pipelineframework.deployment.core.EnvironmentConfiguration;
import org.pipelineframework.deployment.core.EnvironmentCredentialResolver;
import org.pipelineframework.deployment.release.CredentialResolver;
import org.pipelineframework.deployment.target.cloud.CloudArtifactUploader;
import org.pipelineframework.deployment.target.cloud.CloudTargetConfiguration;
import picocli.CommandLine.Command;
import picocli.CommandLine.Model.CommandSpec;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;
import picocli.CommandLine.Spec;

@Command(
    mixinStandardHelpOptions = true,
    name = "upload",
    description = "Upload an immutable artifact ZIP via TPF Cloud presigned intake without mutating any Release descriptor.")
final class ArtifactUploadCommand implements Callable<Integer> {
    @Parameters(index = "0", description = "Named tpf-cloud environment from tpf-deploy.yaml")
    private String environment;
    @Option(names = "--file", required = true, description = "Path to the immutable artifact ZIP file")
    private Path file;
    @Option(names = "--artifact-id", required = true, description = "Artifact identifier matching the pipeline release")
    private String artifactId;
    @Option(names = "--config", defaultValue = "tpf-deploy.yaml", description = "Path to tpf-deploy.yaml")
    private Path config;
    @Option(names = "--output", defaultValue = "human", description = "Output format: human or json")
    private String output;
    @Spec private CommandSpec spec;

    private final CredentialResolver credentials;

    ArtifactUploadCommand() {
        this(new EnvironmentCredentialResolver());
    }

    ArtifactUploadCommand(CredentialResolver credentials) {
        this.credentials = Objects.requireNonNull(credentials, "credentials");
    }

    @Override
    public Integer call() {
        PrintWriter out = spec.commandLine().getOut();
        PrintWriter error = spec.commandLine().getErr();
        try {
            CliSupport.requireOutput(output);
            var loadedConfiguration = CliSupport.configuration(config);
            EnvironmentConfiguration envConfig = loadedConfiguration.environments().get(environment);
            if (envConfig == null) {
                throw new IllegalArgumentException("Unknown environment " + environment);
            }
            if (!"tpf-cloud".equals(envConfig.targetType())) {
                throw new IllegalArgumentException(
                    "Artifact upload requires a tpf-cloud environment; found target type " + envConfig.targetType());
            }
            CloudTargetConfiguration cloudConfig =
                CliSupport.JSON.convertValue(envConfig.targetConfiguration(), CloudTargetConfiguration.class);
            CloudArtifactUploader uploader = new CloudArtifactUploader(cloudConfig, credentials);
            CloudArtifactUploader.UploadResult result = uploader.upload(artifactId, file);

            Map<String, Object> response = new LinkedHashMap<>();
            response.put("operation", "artifact.upload");
            response.put("environment", environment);
            response.put("status", result.status());
            response.put("artifactId", result.artifactId());
            response.put("artifactUri", result.artifactUri());
            response.put("artifactDigest", result.artifactDigest());
            response.put("contentLength", result.contentLength());
            if (!result.bucket().isBlank()) {
                response.put("bucket", result.bucket());
            }
            if (!result.key().isBlank()) {
                response.put("key", result.key());
            }
            if (!result.versionId().isBlank()) {
                response.put("versionId", result.versionId());
            }

            if (output.equals("json")) {
                CliSupport.json(out, response);
            } else {
                out.printf(
                    "%s %s -> %s (%s, %d bytes)%n",
                    result.status(), result.artifactId(), result.artifactUri(),
                    result.artifactDigest(), result.contentLength());
            }
            return 0;
        } catch (Exception e) {
            return CliSupport.failure(e, error);
        }
    }
}
