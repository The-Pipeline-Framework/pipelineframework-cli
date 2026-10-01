package org.pipelineframework.cli;

import java.io.PrintWriter;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.Callable;
import org.pipelineframework.deployment.api.DeploymentTargetProvider;
import org.pipelineframework.deployment.core.DeploymentEngine;
import org.pipelineframework.deployment.core.EnvironmentCredentialResolver;
import org.pipelineframework.deployment.target.cloud.CloudTargetProvider;
import org.pipelineframework.deployment.target.local.LocalProcessTargetProvider;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;
import picocli.CommandLine.Spec;
import picocli.CommandLine.Model.CommandSpec;

@Command(name = "deploy", description = "Deploy an already-produced immutable Release.")
final class DeployCommand implements Callable<Integer> {
    @Parameters(index = "0", description = "Named environment from tpf-deploy.yaml") private String environment;
    @Option(names = "--release") private Path release;
    @Option(names = "--config", defaultValue = "tpf-deploy.yaml") private Path config;
    @Option(names = "--output", defaultValue = "human") private String output;
    @Spec private CommandSpec spec;

    @Override
    public Integer call() {
        PrintWriter out = spec.commandLine().getOut();
        PrintWriter error = spec.commandLine().getErr();
        try {
            CliSupport.requireOutput(output);
            Path descriptor = new ReleasePathLocator().locate(release);
            var loadedConfiguration = CliSupport.configuration(config);
            var credentials = new EnvironmentCredentialResolver();
            Map<String, DeploymentTargetProvider<?>> providers = Map.of(
                "local-process", new LocalProcessTargetProvider(),
                "tpf-cloud", new CloudTargetProvider());
            DeploymentEngine engine = new DeploymentEngine(CliSupport.JSON, providers, credentials);
            var result = engine.deploy(environment, descriptor, Path.of(".tpf"), loadedConfiguration);
            var releaseIdentity = new org.pipelineframework.deployment.release.ReleaseLoader().load(descriptor);
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
}
