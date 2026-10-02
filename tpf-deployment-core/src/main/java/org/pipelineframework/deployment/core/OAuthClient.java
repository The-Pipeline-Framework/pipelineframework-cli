package org.pipelineframework.deployment.core;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.*;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Map;
import java.util.stream.Collectors;

/** Public OAuth client. No Cloud domain policy, management key or embedded secret. */
public final class OAuthClient {
    private static final ObjectMapper JSON = new ObjectMapper();
    private final URI issuer;
    private final HttpClient http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10))
            .followRedirects(HttpClient.Redirect.NEVER).build();

    public OAuthClient(URI issuer) {
        String host = issuer.getHost();
        boolean loopback = "localhost".equals(host) || "127.0.0.1".equals(host) || "[::1]".equals(host);
        if (host == null || issuer.getUserInfo() != null || issuer.getQuery() != null || issuer.getFragment() != null
                || !("https".equals(issuer.getScheme()) || "http".equals(issuer.getScheme()) && loopback)
                || !(issuer.getPath().isEmpty() || issuer.getPath().equals("/")))
            throw new IllegalArgumentException("OAuth issuer must be an HTTPS origin (HTTP loopback is test-only)");
        this.issuer = URI.create(issuer.getScheme() + "://" + issuer.getRawAuthority());
    }

    public JsonNode request(String operation, Map<String,String> parameters) {
        if (!operation.equals("token") && !operation.equals("device_authorization")) throw new IllegalArgumentException("Unsupported OAuth operation");
        String body = parameters.entrySet().stream().map(e -> encode(e.getKey()) + "=" + encode(e.getValue())).collect(Collectors.joining("&"));
        try {
            var response = http.send(HttpRequest.newBuilder(issuer.resolve("/oauth2/" + operation))
                    .timeout(Duration.ofSeconds(30)).header("Content-Type", "application/x-www-form-urlencoded")
                    .POST(HttpRequest.BodyPublishers.ofString(body)).build(), HttpResponse.BodyHandlers.ofString());
            if (response.body().length() > 65536) throw new IllegalStateException("OAuth response is invalid");
            JsonNode result = JSON.readTree(response.body());
            if (response.statusCode() >= 200 && response.statusCode() < 300 && result != null && result.isObject()) return result;
            String code = result == null ? "" : result.path("error").asText();
            // Only known protocol codes cross the boundary; never echo error_description/body.
            if (java.util.Set.of("authorization_pending", "slow_down", "access_denied", "expired_token", "invalid_grant").contains(code))
                throw new OAuthFailure(code);
            throw new OAuthFailure("unavailable");
        } catch (OAuthFailure safe) { throw safe; }
        catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt(); throw new OAuthFailure("interrupted");
        } catch (Exception failure) { throw new OAuthFailure("unavailable"); }
    }
    private static String encode(String text) { return URLEncoder.encode(text, StandardCharsets.UTF_8); }
    public static String text(JsonNode node, String name) {
        if (!node.path(name).isTextual() || node.path(name).asText().isBlank()) throw new OAuthFailure("invalid_response");
        return node.path(name).asText();
    }
    public static final class OAuthFailure extends RuntimeException {
        public final String code;
        public OAuthFailure(String code) { super("OAuth authentication failed (" + code + "); retry or run tpf auth login."); this.code = code; }
    }
}
