package org.pipelineframework.deployment.target.cloud;

import java.net.URI;

public record CloudTargetConfiguration(
    URI endpoint,
    String organization,
    String application,
    String environment,
    String mode,
    String credential
) {
    public CloudTargetConfiguration {
        if (endpoint == null || organization == null || organization.isBlank() || application == null
            || application.isBlank() || environment == null || environment.isBlank()
            || credential == null || credential.isBlank()) {
            throw new IllegalArgumentException("TPF Cloud endpoint, identity and credential are required");
        }
        String scheme = endpoint.getScheme();
        String host = endpoint.getHost();
        if (host == null || host.isBlank()
            || !("https".equalsIgnoreCase(scheme) || ("http".equalsIgnoreCase(scheme) && isLoopback(host)))) {
            throw new IllegalArgumentException("TPF Cloud endpoint must use HTTPS, except HTTP loopback testing");
        }
        mode = mode == null || mode.isBlank() ? "CUSTOMER_MANAGED" : mode;
        if (!mode.equals("CUSTOMER_MANAGED")) {
            throw new IllegalArgumentException("The initial TPF Cloud target supports CUSTOMER_MANAGED only");
        }
    }

    private static boolean isLoopback(String host) {
        String normalized = host.startsWith("[") && host.endsWith("]")
            ? host.substring(1, host.length() - 1)
            : host;
        if (normalized.equalsIgnoreCase("localhost") || normalized.equals("::1")
            || normalized.equals("0:0:0:0:0:0:0:1")) {
            return true;
        }
        String[] octets = normalized.split("\\.", -1);
        if (octets.length != 4 || !octets[0].equals("127")) return false;
        try {
            for (String octet : octets) {
                int value = Integer.parseInt(octet);
                if (value < 0 || value > 255) return false;
            }
            return true;
        } catch (NumberFormatException ignored) {
            return false;
        }
    }
}
