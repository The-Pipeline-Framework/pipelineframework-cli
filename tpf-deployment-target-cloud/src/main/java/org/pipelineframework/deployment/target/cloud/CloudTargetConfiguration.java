package org.pipelineframework.deployment.target.cloud;

import java.net.URI;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;

public record CloudTargetConfiguration(
    URI endpoint,
    String organization,
    String application,
    String environment,
    String mode,
    String credential,
    Integer pollIntervalSeconds,
    Integer timeoutSeconds
) {
    public static final String MODE_CUSTOMER_MANAGED = "CUSTOMER_MANAGED";
    public static final String MODE_TPF_HOSTED = "TPF_HOSTED";
    public static final int DEFAULT_POLL_INTERVAL_SECONDS = 5;
    public static final int MAX_POLL_INTERVAL_SECONDS = 60;
    public static final int DEFAULT_TIMEOUT_SECONDS = 300;
    public static final int MAX_TIMEOUT_SECONDS = 3600;

    public CloudTargetConfiguration(
        URI endpoint,
        String organization,
        String application,
        String environment,
        String mode,
        String credential
    ) {
        this(endpoint, organization, application, environment, mode, credential, null, null);
    }

    public CloudTargetConfiguration {
        if (endpoint == null || organization == null || organization.isBlank() || application == null
            || application.isBlank() || environment == null || environment.isBlank()
            || credential == null || credential.isBlank()) {
            throw new IllegalArgumentException("TPF Cloud endpoint, identity and credential are required");
        }
        validateEndpointUri(endpoint, "TPF Cloud endpoint");
        mode = mode == null || mode.isBlank() ? MODE_CUSTOMER_MANAGED : mode.trim();
        if (!mode.equals(MODE_CUSTOMER_MANAGED) && !mode.equals(MODE_TPF_HOSTED)) {
            throw new IllegalArgumentException(
                "Unsupported TPF Cloud deployment mode " + mode + "; expected CUSTOMER_MANAGED or TPF_HOSTED");
        }
        pollIntervalSeconds = pollIntervalSeconds == null ? DEFAULT_POLL_INTERVAL_SECONDS : pollIntervalSeconds;
        timeoutSeconds = timeoutSeconds == null ? DEFAULT_TIMEOUT_SECONDS : timeoutSeconds;
        if (pollIntervalSeconds < 1 || pollIntervalSeconds > MAX_POLL_INTERVAL_SECONDS) {
            throw new IllegalArgumentException(
                "TPF Cloud pollIntervalSeconds must be between 1 and " + MAX_POLL_INTERVAL_SECONDS);
        }
        if (timeoutSeconds < 1 || timeoutSeconds > MAX_TIMEOUT_SECONDS) {
            throw new IllegalArgumentException(
                "TPF Cloud timeoutSeconds must be between 1 and " + MAX_TIMEOUT_SECONDS);
        }
    }

    public boolean isHosted() {
        return MODE_TPF_HOSTED.equals(mode);
    }

    URI endpointUri(String path) {
        String basePath = endpoint.getRawPath();
        if (basePath == null || basePath.isBlank() || basePath.equals("/")) {
            basePath = "";
        } else if (basePath.endsWith("/")) {
            basePath = basePath.substring(0, basePath.length() - 1);
        }
        String query = endpoint.getRawQuery() == null ? "" : "?" + endpoint.getRawQuery();
        return URI.create(endpoint.getScheme() + "://" + endpoint.getRawAuthority() + basePath + path + query);
    }

    static String encodePathSegment(String value) {
        return URLEncoder.encode(value, StandardCharsets.UTF_8).replace("+", "%20");
    }

    static void validateEndpointUri(URI uri, String label) {
        String scheme = uri.getScheme();
        String host = uri.getHost();
        if (host == null || host.isBlank()
            || !("https".equalsIgnoreCase(scheme) || ("http".equalsIgnoreCase(scheme) && isLoopback(host)))) {
            throw new IllegalArgumentException(label + " must use HTTPS, except HTTP loopback testing");
        }
    }

    static boolean isLoopback(String host) {
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
