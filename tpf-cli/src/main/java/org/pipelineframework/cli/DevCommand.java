package org.pipelineframework.cli;

import java.io.PrintWriter;
import java.net.URI;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;

import org.pipelineframework.TpfLocalDevRuntime;
import picocli.CommandLine.Command;
import picocli.CommandLine.Model.CommandSpec;
import picocli.CommandLine.Option;
import picocli.CommandLine.Spec;

/**
 * Local {@code tpf dev} command (ADR-0092): embeds TPF's standard Java execution engine
 * ({@code QueueAsyncCoordinator}, {@code PipelineExecutionService}, {@code CanonicalFunctionHostStep},
 * {@code PolyglotConnectorProvider}, {@code AwaitCoordinator}) backed by crash-safe fsynced disk
 * stores for zero-Docker, zero-JDK local development and {@code SIGKILL} recovery.
 */
@Command(
    mixinStandardHelpOptions = true,
    name = "dev",
    description = "Run the crash-safe local TPF execution engine against a foreign function/connector host.")
final class DevCommand implements Callable<Integer> {

    @Option(names = {"-p", "--pipeline"}, required = true, description = "Path to canonical v3 pipeline.yaml.")
    private Path pipeline;

    @Option(names = "--function-host", required = true, description = "Base URI of the foreign function host (e.g. http://127.0.0.1:4100).")
    private URI functionHost;

    @Option(names = "--connector-host", description = "Optional base URI of the Tier-2 custom connector host (defaults to --function-host).")
    private URI connectorHost;

    @Option(names = "--secret", description = "Shared HMAC-SHA256 secret (or set TPF_DEV_SECRET).")
    private String secret;

    @Option(names = "--data-dir", defaultValue = ".tpf/dev-state", description = "Directory for crash-safe fsynced execution and Await state.")
    private Path dataDir;

    @Option(names = "--port", defaultValue = "0", description = "Local HTTP port to bind on 127.0.0.1 (0 picks an ephemeral port).")
    private int port;

    @Option(names = "--max-retries", defaultValue = "2", description = "Coordinator max retry attempts for transient failures.")
    private int maxRetries;

    @Option(names = "--connector-secret", description = "Host-owned Tier-2 connector secret pair (key=value; repeatable).")
    private Map<String, String> connectorSecrets = new LinkedHashMap<>();

    @Option(names = "--allow-loopback-http", defaultValue = "true", negatable = true,
        description = "Allow plaintext HTTP on loopback addresses for local development (default: true).")
    private boolean allowLoopbackHttp;

    @Spec
    private CommandSpec spec;

    @Override
    public Integer call() {
        long startNanos = System.nanoTime();
        PrintWriter out = spec.commandLine().getOut();
        PrintWriter err = spec.commandLine().getErr();
        try {
            String effectiveSecret = resolveSecret(secret);
            URI effectiveConnectorHost = connectorHost != null ? connectorHost : functionHost;
            TpfLocalDevRuntime.initializeNativeBootstrap();
            TpfLocalDevRuntime runtime = new TpfLocalDevRuntime(
                pipeline,
                functionHost,
                Optional.of(effectiveConnectorHost),
                effectiveSecret,
                dataDir,
                maxRetries,
                Map.copyOf(connectorSecrets),
                allowLoopbackHttp);
            var server = runtime.startHttp(port, startNanos, out);
            CountDownLatch stop = new CountDownLatch(1);
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                server.stop(0);
                stop.countDown();
            }));
            stop.await();
            return 0;
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            return 0;
        } catch (Exception e) {
            return CliSupport.failure(e, err);
        }
    }

    private static String resolveSecret(String flagValue) {
        if (flagValue != null && !flagValue.isBlank()) {
            return flagValue;
        }
        String env = System.getenv("TPF_DEV_SECRET");
        if (env != null && !env.isBlank()) {
            return env;
        }
        return "tpf-local-dev-secret";
    }
}
