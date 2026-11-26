package org.pipelineframework.cli;

import java.io.PrintWriter;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.Callable;

import org.pipelineframework.compiler.polyglot.PolyglotCompiler;
import picocli.CommandLine.Command;
import picocli.CommandLine.Model.CommandSpec;
import picocli.CommandLine.Option;
import picocli.CommandLine.Spec;

/**
 * Offline {@code tpf generate} command (ADR-0092): compiles a canonical v3 {@code pipeline.yaml}
 * into class-free {@code META-INF/pipeline/pipeline-contract.json}, {@code polyglot-manifest.json},
 * and thin TypeScript or Python function/connector stubs with zero JDK or Maven required in the
 * native distribution.
 */
@Command(
    mixinStandardHelpOptions = true,
    name = "generate",
    description = "Compile a v3 pipeline.yaml into class-free contracts and thin TypeScript or Python stubs.")
final class GenerateCommand implements Callable<Integer> {

    @Option(names = {"-p", "--pipeline"}, required = true, description = "Path to canonical v3 pipeline.yaml.")
    private Path pipeline;

    @Option(names = {"-l", "--lang"}, defaultValue = "ts", description = "Target language: ts (typescript) or py (python).")
    private String lang;

    @Option(names = {"-o", "--out"}, required = true, description = "Output directory for generated contracts and stubs.")
    private Path out;

    @Option(names = "--output", defaultValue = "human", description = "Output format: human or json.")
    private String output;

    @Spec
    private CommandSpec spec;

    @Override
    public Integer call() {
        long startNanos = System.nanoTime();
        PrintWriter outWriter = spec.commandLine().getOut();
        PrintWriter errWriter = spec.commandLine().getErr();
        try {
            CliSupport.requireOutput(output);
            PolyglotCompiler.Target target = parseTarget(lang);
            PolyglotCompiler.CompileResult compiled = PolyglotCompiler.compile(target, pipeline, out);
            long elapsedMs = (System.nanoTime() - startNanos) / 1_000_000L;

            Map<String, Object> result = new LinkedHashMap<>();
            result.put("operation", "generate");
            result.put("status", "GENERATED");
            result.put("pipelineId", compiled.pipelineId());
            result.put("target", target.id());
            result.put("contractVersion", compiled.contractVersion());
            result.put("catalogFingerprint", compiled.catalogFingerprint());
            result.put("outputDir", out.toAbsolutePath().normalize().toString());
            result.put("generatedFiles", compiled.generatedFiles());
            result.put("elapsedMs", elapsedMs);

            if ("json".equals(output)) {
                CliSupport.json(outWriter, result);
            } else {
                outWriter.printf(
                    "Generated %s (%s, %d files) in %dms -> %s%n",
                    compiled.pipelineId(),
                    target.id(),
                    compiled.generatedFiles().size(),
                    elapsedMs,
                    out.toAbsolutePath().normalize());
            }
            return 0;
        } catch (Exception e) {
            return CliSupport.failure(e, errWriter);
        }
    }

    private static PolyglotCompiler.Target parseTarget(String raw) {
        if (raw == null || raw.isBlank()) {
            throw new IllegalArgumentException("--lang must be ts, typescript, py, or python");
        }
        return switch (raw.trim().toLowerCase(Locale.ROOT)) {
            case "ts", "typescript" -> PolyglotCompiler.Target.TYPESCRIPT;
            case "py", "python" -> PolyglotCompiler.Target.PYTHON;
            default -> throw new IllegalArgumentException(
                "Unsupported --lang '" + raw + "' (expected ts, typescript, py, or python)");
        };
    }
}
