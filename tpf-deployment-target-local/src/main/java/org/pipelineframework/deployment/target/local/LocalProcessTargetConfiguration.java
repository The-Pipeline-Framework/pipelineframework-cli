package org.pipelineframework.deployment.target.local;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;

public record LocalProcessTargetConfiguration(Path workspace, String javaExecutable, List<Unit> units) {
    public LocalProcessTargetConfiguration {
        workspace = workspace == null ? Path.of(".tpf", "deployments", "local") : workspace;
        javaExecutable = javaExecutable == null || javaExecutable.isBlank() ? "java" : javaExecutable;
        units = units == null ? List.of() : List.copyOf(units);
        if (units.isEmpty()) throw new IllegalArgumentException("local-process requires at least one unit");
    }

    public record Unit(String artifactId, List<String> arguments, Map<String, String> environment, Readiness readiness) {
        public Unit {
            if (artifactId == null || artifactId.isBlank()) throw new IllegalArgumentException("Unit artifactId is required");
            arguments = arguments == null ? List.of() : List.copyOf(arguments);
            environment = environment == null ? Map.of() : Map.copyOf(environment);
            readiness = readiness == null ? new Readiness("", 30) : readiness;
        }
    }

    public record Readiness(String http, int timeoutSeconds) {
        public Readiness {
            http = http == null ? "" : http;
            if (timeoutSeconds <= 0) timeoutSeconds = 30;
        }
    }
}
