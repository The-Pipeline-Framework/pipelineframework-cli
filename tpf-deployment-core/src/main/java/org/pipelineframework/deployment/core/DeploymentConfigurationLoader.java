package org.pipelineframework.deployment.core;

import com.fasterxml.jackson.core.StreamReadFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.pipelineframework.deployment.release.CredentialReference;
import org.pipelineframework.deployment.release.MavenResolverConfiguration;
import org.pipelineframework.deployment.release.OciResolverConfiguration;
import org.pipelineframework.deployment.release.ResolverProfile;

/** Strict YAML loader for build-tool-neutral deployment environments. */
public final class DeploymentConfigurationLoader {
    private static final ObjectMapper YAML = new ObjectMapper(
        YAMLFactory.builder().enable(StreamReadFeature.STRICT_DUPLICATE_DETECTION).build());

    public DeploymentConfiguration load(Path source) throws IOException {
        JsonNode parsed = YAML.readTree(source.toFile());
        ObjectNode root = object(parsed, "configuration");
        rejectUnknown(root, Set.of("resolverProfiles", "environments"), "configuration");
        Map<String, ResolverProfile> profiles = profiles(object(required(root, "resolverProfiles"), "resolverProfiles"));
        Map<String, EnvironmentConfiguration> environments = environments(
            object(required(root, "environments"), "environments"));
        environments.forEach((name, environment) -> {
            if (!profiles.containsKey(environment.resolverProfile())) {
                throw new IllegalArgumentException(
                    "Environment " + name + " references unknown resolver profile " + environment.resolverProfile());
            }
        });
        return new DeploymentConfiguration(profiles, environments);
    }

    private static Map<String, ResolverProfile> profiles(ObjectNode node) {
        Map<String, ResolverProfile> result = new LinkedHashMap<>();
        node.fields().forEachRemaining(entry -> {
            ObjectNode profile = object(entry.getValue(), "resolver profile " + entry.getKey());
            rejectUnknown(profile, Set.of("file", "maven", "oci"), "resolver profile " + entry.getKey());
            boolean file = !profile.has("file") || profile.path("file").asBoolean(true);
            Optional<MavenResolverConfiguration> maven = Optional.ofNullable(profile.get("maven"))
                .map(value -> maven(object(value, "maven resolver")));
            Optional<OciResolverConfiguration> oci = Optional.ofNullable(profile.get("oci"))
                .map(value -> oci(object(value, "oci resolver")));
            result.put(entry.getKey(), new ResolverProfile(file, maven, oci));
        });
        return result;
    }

    private static MavenResolverConfiguration maven(ObjectNode node) {
        rejectUnknown(node, Set.of("settings", "localRepository", "repositories"), "maven resolver");
        Path settings = node.hasNonNull("settings") ? expandHome(node.path("settings").asText()) : null;
        Path local = node.hasNonNull("localRepository") ? expandHome(node.path("localRepository").asText()) : null;
        List<URI> repositories = node.has("repositories")
            ? iterable(node.path("repositories")).stream().map(value -> URI.create(value.asText())).toList()
            : List.of();
        return new MavenResolverConfiguration(settings, local, repositories);
    }

    private static OciResolverConfiguration oci(ObjectNode node) {
        rejectUnknown(node, Set.of("credentials", "insecureRegistries"), "oci resolver");
        Map<String, CredentialReference> credentials = new LinkedHashMap<>();
        String source = "";
        if (node.has("credentials")) {
            JsonNode configured = node.path("credentials");
            if (configured.isTextual()) {
                source = text(configured, "oci credentials");
                if (!source.equals("docker-config")) {
                    throw new IllegalArgumentException("OCI credentials must be docker-config or a registry map");
                }
            } else {
                ObjectNode values = object(configured, "oci credentials");
                values.fields().forEachRemaining(entry ->
                    credentials.put(entry.getKey(), new CredentialReference(entry.getValue().asText())));
            }
        }
        Set<String> insecure = node.has("insecureRegistries")
            ? iterable(node.path("insecureRegistries")).stream().map(JsonNode::asText).collect(java.util.stream.Collectors.toSet())
            : Set.of();
        return new OciResolverConfiguration(source, credentials, insecure);
    }

    private static Map<String, EnvironmentConfiguration> environments(ObjectNode node) {
        Map<String, EnvironmentConfiguration> result = new LinkedHashMap<>();
        node.fields().forEachRemaining(entry -> {
            ObjectNode environment = object(entry.getValue(), "environment " + entry.getKey());
            rejectUnknown(environment, Set.of("resolverProfile", "target"), "environment " + entry.getKey());
            ObjectNode target = object(required(environment, "target"), "target");
            String type = text(required(target, "type"), "target.type");
            ObjectNode targetCopy = target.deepCopy();
            targetCopy.remove("type");
            result.put(entry.getKey(), new EnvironmentConfiguration(
                text(required(environment, "resolverProfile"), "resolverProfile"), type, targetCopy));
        });
        return result;
    }

    private static Path expandHome(String value) {
        if (value.equals("~")) return Path.of(System.getProperty("user.home"));
        if (value.startsWith("~/")) return Path.of(System.getProperty("user.home"), value.substring(2));
        return Path.of(value);
    }

    private static JsonNode required(ObjectNode node, String name) {
        JsonNode value = node.get(name);
        if (value == null || value.isNull()) throw new IllegalArgumentException(name + " is required");
        return value;
    }

    private static String text(JsonNode node, String name) {
        if (!node.isTextual() || node.asText().isBlank()) throw new IllegalArgumentException(name + " must be text");
        return node.asText().trim();
    }

    private static ObjectNode object(JsonNode node, String name) {
        if (!(node instanceof ObjectNode object)) throw new IllegalArgumentException(name + " must be an object");
        return object;
    }

    private static List<JsonNode> iterable(JsonNode node) {
        if (!node.isArray()) throw new IllegalArgumentException("Expected an array");
        return java.util.stream.StreamSupport.stream(node.spliterator(), false).toList();
    }

    private static void rejectUnknown(ObjectNode node, Set<String> allowed, String name) {
        Iterator<String> fields = node.fieldNames();
        while (fields.hasNext()) {
            String field = fields.next();
            if (!allowed.contains(field)) throw new IllegalArgumentException("Unknown " + name + " key " + field);
        }
    }
}
