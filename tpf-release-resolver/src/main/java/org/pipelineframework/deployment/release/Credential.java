package org.pipelineframework.deployment.release;

/** Authentication material supplied by the deployment environment, never by a Release. */
public sealed interface Credential permits Credential.Basic, Credential.Bearer {
    record Basic(String username, String password) implements Credential {
        public Basic {
            if (username == null || password == null) {
                throw new IllegalArgumentException("Basic credential requires username and password");
            }
        }
    }

    record Bearer(String token) implements Credential {
        public Bearer {
            if (token == null || token.isBlank()) {
                throw new IllegalArgumentException("Bearer credential token is required");
            }
        }
    }
}
