package org.pipelineframework.cli;

import java.net.URI;
import java.nio.file.Path;
import java.time.Instant;
import java.util.Map;
import java.util.concurrent.Callable;
import org.pipelineframework.deployment.core.OAuthClient;
import org.pipelineframework.deployment.core.UserCredentialDirectory;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Spec;
import picocli.CommandLine.Model.CommandSpec;

@Command(name="auth", mixinStandardHelpOptions=true, description="Manage human Cloud credentials (deploy never prompts).",
        subcommands={AuthCommand.Login.class, AuthCommand.Status.class, AuthCommand.Logout.class})
final class AuthCommand implements Runnable {
    @Spec CommandSpec spec;
    @Override public void run() { spec.commandLine().usage(spec.commandLine().getOut()); }

    static abstract class Credentials implements Callable<Integer> {
        @Option(names="--profile", defaultValue="cloud") String profile;
        @Option(names="--credential-dir", description="Mounted user credential directory") Path directory;
        @Spec CommandSpec spec;
        UserCredentialDirectory store() {
            return new UserCredentialDirectory(directory == null ? UserCredentialDirectory.configured(System.getenv()) : directory);
        }
        int failure() {
            spec.commandLine().getErr().println("tpf: Cloud authentication failed. Check the credential mount, retry, or run tpf auth login. No tokens printed.");
            return CliSupport.AUTH_FAILURE;
        }
    }
    @Command(name="login", mixinStandardHelpOptions=true, description="Explicit public-client device authorization; no embedded client secret.")
    static final class Login extends Credentials {
        @Option(names="--issuer", required=true) URI issuer;
        @Option(names="--client-id", required=true) String clientId;
        @Option(names="--verification-host", description="Additional trusted verification host (exact hostname; repeatable)")
        java.util.Set<String> verificationHosts = new java.util.HashSet<>();
        boolean trustedVerificationHost(String host) {
            return host != null && (host.equalsIgnoreCase(issuer.getHost())
                    || verificationHosts.stream().anyMatch(host::equalsIgnoreCase));
        }
        @Override public Integer call() {
            try {
                var client = new OAuthClient(issuer);
                var device = client.request("device_authorization", Map.of("client_id", clientId, "scope", "openid offline_access tpf:deploy"));
                String deviceCode = OAuthClient.text(device, "device_code");
                URI verification = URI.create(OAuthClient.text(device, "verification_uri"));
                if (!"https".equals(verification.getScheme()) && !("http".equals(verification.getScheme()) && "127.0.0.1".equals(verification.getHost())))
                    return failure();
                if (!trustedVerificationHost(verification.getHost()) || verification.getUserInfo() != null) return failure();
                long ttl = device.path("expires_in").asLong(0);
                int interval = device.path("interval").asInt(5);
                if (ttl <= 0 || ttl > 3600 || interval < 1 || interval > 60) return failure();
                var end = Instant.now().plusSeconds(ttl);
                // User code is the intended device-flow prompt, never an access/refresh/device token.
                spec.commandLine().getOut().printf("Open %s and enter code %s.%n", verification, OAuthClient.text(device, "user_code"));
                spec.commandLine().getOut().flush();
                while (Instant.now().isBefore(end)) {
                    Thread.sleep(interval * 1000L);
                    if (!Instant.now().isBefore(end)) return failure();
                    try {
                        var tokens = client.request("token", Map.of("grant_type", "urn:ietf:params:oauth:grant-type:device_code",
                                "client_id", clientId, "device_code", deviceCode));
                        store().save(profile, issuer, clientId, tokens);
                        spec.commandLine().getOut().println("Signed in. Credentials stored only in the user credential directory.");
                        return 0;
                    } catch (OAuthClient.OAuthFailure protocol) {
                        if (protocol.code.equals("slow_down")) interval += 5;
                        else if (!protocol.code.equals("authorization_pending")) return failure();
                    }
                }
                return failure();
            } catch (InterruptedException interrupted) { Thread.currentThread().interrupt(); return failure(); }
            catch (Exception rejected) { return failure(); }
        }
    }
    @Command(name="status", mixinStandardHelpOptions=true, description="Check credentials without printing tokens; refresh if needed.")
    static final class Status extends Credentials {
        @Override public Integer call() {
            try {
                boolean present = store().resolve(profile).isPresent();
                spec.commandLine().getOut().println(present ? "Signed in (local credentials available)." : "Not signed in. Run tpf auth login.");
                return present ? 0 : CliSupport.AUTH_FAILURE;
            } catch (Exception unavailable) { return failure(); }
        }
    }
    @Command(name="logout", mixinStandardHelpOptions=true, description="Remove local human credentials; does not revoke issued provider tokens.")
    static final class Logout extends Credentials {
        @Override public Integer call() {
            try { store().logout(profile); spec.commandLine().getOut().println("Signed out locally. Human credentials removed."); return 0; }
            catch (Exception unavailable) { return failure(); }
        }
    }
}
