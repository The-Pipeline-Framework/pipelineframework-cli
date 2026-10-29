package org.pipelineframework.cli;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;
import java.util.stream.StreamSupport;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.pipelineframework.orchestrator.release.PipelineContractDescriptor;

class NativeReachabilityMetadataTest {
    @Test
    void registersTheCurrentContractDescriptorCanonicalConstructor() throws Exception {
        String resource = "META-INF/native-image/org.pipelineframework/tpf-cli/reachability-metadata.json";
        try (var input = getClass().getClassLoader().getResourceAsStream(resource)) {
            assertNotNull(input, "Native reachability metadata must be packaged with the CLI");
            var metadata = new ObjectMapper().readTree(input);
            var registration = StreamSupport.stream(metadata.path("reflection").spliterator(), false)
                .filter(entry -> PipelineContractDescriptor.class.getName().equals(entry.path("type").asText()))
                .findFirst().orElseThrow();
            var constructors = StreamSupport.stream(registration.path("methods").spliterator(), false)
                .filter(method -> "<init>".equals(method.path("name").asText()))
                .map(method -> StreamSupport.stream(method.path("parameterTypes").spliterator(), false)
                    .map(JsonNode::asText).toList())
                .toList();
            var canonicalSignature = Arrays.stream(PipelineContractDescriptor.class.getRecordComponents())
                .map(component -> component.getType().getName()).toList();
            assertTrue(constructors.contains(canonicalSignature),
                () -> "Native metadata must register the canonical contract constructor: " + canonicalSignature);
        }
    }
}
