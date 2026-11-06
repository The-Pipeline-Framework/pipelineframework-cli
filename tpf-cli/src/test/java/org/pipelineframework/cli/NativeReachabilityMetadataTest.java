package org.pipelineframework.cli;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;
import java.util.List;
import java.util.stream.StreamSupport;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.pipelineframework.orchestrator.release.PipelineContractDescriptor;
import org.pipelineframework.orchestrator.release.PipelineReleaseDeployment;
import org.pipelineframework.orchestrator.release.PipelineReleaseDescriptor;

class NativeReachabilityMetadataTest {
    @Test
    void registersCanonicalConstructorsForTheReleaseClosure() throws Exception {
        var metadata = metadata();
        for (var record : List.of(PipelineContractDescriptor.class, PipelineReleaseDescriptor.class,
            PipelineReleaseDeployment.class, PipelineReleaseDeployment.Entry.class,
            PipelineReleaseDeployment.Binding.class, PipelineReleaseDeployment.Check.class)) {
            var constructors = StreamSupport.stream(registration(metadata, record.getName()).path("methods").spliterator(), false)
                .filter(method -> "<init>".equals(method.path("name").asText()))
                .map(method -> StreamSupport.stream(method.path("parameterTypes").spliterator(), false)
                    .map(JsonNode::asText).toList())
                .toList();
            var signature = Arrays.stream(record.getRecordComponents())
                .map(component -> component.getType().getName()).toList();
            assertTrue(constructors.contains(signature),
                () -> "Native metadata must register the canonical constructor of " + record.getName() + ": " + signature);
        }
    }

    @Test
    void registersTheReleaseMixinThatOmitsAbsentDeploymentFromSchemaOne() throws Exception {
        var mixin = registration(metadata(), "org.pipelineframework.config.pipeline.PipelineJson$PipelineReleaseDescriptorMixin");
        assertTrue(StreamSupport.stream(mixin.path("methods").spliterator(), false)
            .anyMatch(method -> "deployment".equals(method.path("name").asText())
                && method.path("parameterTypes").isArray() && method.path("parameterTypes").isEmpty()));
    }

    private static JsonNode metadata() throws Exception {
        String resource = "META-INF/native-image/org.pipelineframework/tpf-cli/reachability-metadata.json";
        try (var input = NativeReachabilityMetadataTest.class.getClassLoader().getResourceAsStream(resource)) {
            assertNotNull(input, "Native reachability metadata must be packaged with the CLI");
            return new ObjectMapper().readTree(input);
        }
    }

    private static JsonNode registration(JsonNode metadata, String type) {
        return StreamSupport.stream(metadata.path("reflection").spliterator(), false)
            .filter(entry -> type.equals(entry.path("type").asText()))
            .findFirst().orElseThrow(() -> new AssertionError("Missing native metadata for " + type));
    }
}
