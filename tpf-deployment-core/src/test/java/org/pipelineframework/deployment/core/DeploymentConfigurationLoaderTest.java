package org.pipelineframework.deployment.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class DeploymentConfigurationLoaderTest {
    @TempDir Path temporaryDirectory;

    @Test
    void loadsStrictBuildToolNeutralNamedEnvironments() throws Exception {
        Path config = write("""
            resolverProfiles:
              default:
                maven:
                  settings: ~/.m2/settings.xml
                  repositories:
                    - https://repo.example.test/releases
                oci:
                  credentials: docker-config
                  insecureRegistries:
                    - 127.0.0.1:5000
            environments:
              local:
                resolverProfile: default
                target:
                  type: local-process
                  workspace: .tpf/deployments/local
                  units:
                    - artifactId: application
              production-eu:
                resolverProfile: default
                target:
                  type: tpf-cloud
                  endpoint: https://api.example.test
                  organization: example
                  application: payments
                  environment: production
                  mode: CUSTOMER_MANAGED
                  credential: production
            """);

        DeploymentConfiguration loaded = new DeploymentConfigurationLoader().load(config);

        assertEquals(2, loaded.environments().size());
        assertEquals("local-process", loaded.environments().get("local").targetType());
        assertEquals("tpf-cloud", loaded.environments().get("production-eu").targetType());
        assertEquals("docker-config", loaded.resolverProfiles().get("default").oci().orElseThrow().credentialSource());
    }

    @Test
    void rejectsUnknownKeysDuplicateNamesAndUnknownProfiles() throws Exception {
        assertThrows(IllegalArgumentException.class, () -> new DeploymentConfigurationLoader().load(write("""
            resolverProfiles:
              default:
                surprise: true
            environments: {}
            """)));
        assertThrows(Exception.class, () -> new DeploymentConfigurationLoader().load(write("""
            resolverProfiles:
              default: {}
              default: {}
            environments: {}
            """)));
        assertThrows(IllegalArgumentException.class, () -> new DeploymentConfigurationLoader().load(write("""
            resolverProfiles:
              default: {}
            environments:
              staging:
                resolverProfile: missing
                target:
                  type: tpf-cloud
            """)));
    }

    private Path write(String value) throws Exception {
        return Files.writeString(temporaryDirectory.resolve(java.util.UUID.randomUUID() + ".yaml"), value);
    }
}
