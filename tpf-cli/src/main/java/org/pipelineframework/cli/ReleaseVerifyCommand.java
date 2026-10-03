package org.pipelineframework.cli;

import java.io.PrintWriter;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.Callable;
import org.pipelineframework.deployment.core.EnvironmentCredentialResolver;
import org.pipelineframework.deployment.release.ReleaseVerifier;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Spec;
import picocli.CommandLine.Model.CommandSpec;

@Command(mixinStandardHelpOptions = true, name = "verify", description = "Resolve and verify a Release from pipeline-release.json.")
final class ReleaseVerifyCommand implements Callable<Integer> {
    @Option(names = "--release") private Path release;
    @Option(names = "--config", defaultValue = "tpf-deploy.yaml") private Path config;
    @Option(names = "--resolver-profile") private String resolverProfile;
    @Option(names = "--output", defaultValue = "human") private String output;
    @Spec private CommandSpec spec;

    @Override
    public Integer call() {
        PrintWriter out = spec.commandLine().getOut();
        PrintWriter error = spec.commandLine().getErr();
        try {
            CliSupport.requireOutput(output);
            Path descriptor = new ReleasePathLocator().locate(release);
            var profile = CliSupport.verificationProfile(config, resolverProfile);
            var verified = new ReleaseVerifier().verify(
                descriptor, Path.of(".tpf", "verification", java.util.UUID.randomUUID().toString()),
                profile, new EnvironmentCredentialResolver());
            Map<String, Object> result = new LinkedHashMap<>();
            result.put("operation", "release.verify");
            result.put("status", "VERIFIED");
            result.put("descriptorDigest", verified.loaded().descriptorDigest());
            result.put("pipelineId", verified.loaded().descriptor().pipelineId());
            result.put("contractVersion", verified.loaded().descriptor().contractVersion());
            result.put("releaseVersion", verified.loaded().descriptor().releaseVersion());
            result.put("artifactCount", verified.resolved().artifacts().size());
            if (output.equals("json")) CliSupport.json(out, result);
            else out.printf(
                "Verified %s/%s/%s (%s, %d artifacts)%n",
                result.get("pipelineId"), result.get("contractVersion"), result.get("releaseVersion"),
                result.get("descriptorDigest"), result.get("artifactCount"));
            return 0;
        } catch (Exception e) {
            return CliSupport.failure(e, error);
        }
    }
}
