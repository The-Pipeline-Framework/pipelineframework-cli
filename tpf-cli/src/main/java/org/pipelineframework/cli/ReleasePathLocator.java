package org.pipelineframework.cli;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

final class ReleasePathLocator {
    Path locate(Path explicit) {
        if (explicit != null) return explicit.toAbsolutePath().normalize();
        List<Path> candidates = new ArrayList<>();
        for (Path candidate : List.of(Path.of("pipeline-release.json"), Path.of("target", "pipeline-release.json"))) {
            if (Files.isRegularFile(candidate)) candidates.add(candidate.toAbsolutePath().normalize());
        }
        if (candidates.isEmpty()) {
            throw new IllegalArgumentException(
                "No Release Descriptor found; use --release or provide ./pipeline-release.json or ./target/pipeline-release.json");
        }
        if (candidates.size() > 1) {
            throw new IllegalArgumentException("Release Descriptor location is ambiguous; use --release");
        }
        return candidates.getFirst();
    }
}
