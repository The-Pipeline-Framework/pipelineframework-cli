package org.pipelineframework.deployment.core;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpServer;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.pipelineframework.deployment.release.Credential;
import org.pipelineframework.deployment.release.CredentialReference;
import static org.junit.jupiter.api.Assertions.*;

class OAuthCredentialsTest {
    @TempDir Path temporary;
    @org.junit.jupiter.api.BeforeEach void realTemporaryRoot() throws Exception { temporary = temporary.toRealPath(); }
    private final ObjectMapper json = new ObjectMapper();
    private com.fasterxml.jackson.databind.JsonNode tokens(String access, String refresh, long ttl) throws Exception {
        return json.readTree(json.writeValueAsBytes(Map.of("access_token",access,"refresh_token",refresh,"expires_in",ttl,"token_type","Bearer")));
    }
    @Test void storesOnlyInOwnerOnlyMountedDirectoryAndLogsOut() throws Exception {
        var directory = temporary.resolve("credentials"); var store = new UserCredentialDirectory(directory);
        store.save("human", URI.create("https://auth.example"), "client_public", tokens("secret-access", "secret-refresh", 600));
        assertEquals(PosixFilePermissions.fromString("rwx------"), Files.getPosixFilePermissions(directory));
        assertEquals(PosixFilePermissions.fromString("rw-------"), Files.getPosixFilePermissions(directory.resolve("human.json")));
        var credential = store.resolve("human").orElseThrow();
        assertFalse(credential.toString().contains("secret-access"));
        assertFalse(new Credential.Basic("user", "secret-password").toString().contains("secret-password"));
        var malformed=assertThrows(IllegalArgumentException.class, () -> new Credential.Bearer("secret-token\nheader"));
        assertFalse(malformed.getMessage().contains("secret-token"));
        store.logout("human"); assertTrue(store.resolve("human").isEmpty()); store.logout("human");
    }
    @Test void refusesSymlinksAndInsecureFiles() throws Exception {
        var directory=temporary.resolve("credentials"); var store=new UserCredentialDirectory(directory);
        store.save("human", URI.create("https://auth.example"), "client_public", tokens("access", "refresh", 600));
        Files.setPosixFilePermissions(directory.resolve("human.json"), PosixFilePermissions.fromString("rw-r--r--"));
        assertThrows(IllegalStateException.class, () -> store.resolve("human"));
        Path link=temporary.resolve("linked"); Files.createSymbolicLink(link, directory);
        assertThrows(IllegalStateException.class, () -> new UserCredentialDirectory(link).resolve("human"));
        assertThrows(IllegalStateException.class, () -> store.resolve("../outside"));
    }
    @Test void refreshRotationSerializesReadersAndDiscardsInvalidGrant() throws Exception {
        AtomicInteger calls=new AtomicInteger(); AtomicInteger invalid=new AtomicInteger();
        var server=HttpServer.create(new InetSocketAddress("127.0.0.1",0),0);
        server.createContext("/oauth2/token", request -> {
            String body=new String(request.getRequestBody().readAllBytes(),StandardCharsets.UTF_8);
            assertTrue(body.contains("grant_type=refresh_token")); assertTrue(body.contains("client_id=client_public"));
            calls.incrementAndGet();
            byte[] response=(invalid.get()==0 ? "{\"access_token\":\"new-access\",\"refresh_token\":\"rotated-refresh\",\"expires_in\":600,\"token_type\":\"Bearer\"}"
                    : "{\"error\":\"invalid_grant\",\"error_description\":\"secret-refresh\"}").getBytes(StandardCharsets.UTF_8);
            request.sendResponseHeaders(invalid.get()==0 ? 200 : 400,response.length); request.getResponseBody().write(response); request.close();
        }); server.start();
        try {
            var issuer=URI.create("http://127.0.0.1:"+server.getAddress().getPort()); var store=new UserCredentialDirectory(temporary.resolve("credentials"));
            store.save("human",issuer,"client_public",tokens("old-access","old-refresh",1));
            var first=java.util.concurrent.CompletableFuture.supplyAsync(() -> store.resolve("human"));
            var second=java.util.concurrent.CompletableFuture.supplyAsync(() -> store.resolve("human"));
            assertTrue(first.join().isPresent()); assertTrue(second.join().isPresent()); assertEquals(1,calls.get());
            String saved=Files.readString(temporary.resolve("credentials/human.json"));
            assertTrue(saved.contains("rotated-refresh")); assertFalse(saved.contains("old-refresh"));
            invalid.set(1); store.save("human",issuer,"client_public",tokens("old-access","old-refresh",1));
            var error=assertThrows(OAuthClient.OAuthFailure.class, () -> store.resolve("human"));
            assertFalse(error.getMessage().contains("secret-refresh")); assertTrue(store.resolve("human").isEmpty());
        } finally { server.stop(0); }
    }
    @Test void ciAcquiresAtInvocationTimeWithoutCredentialFilesOrInteractiveFallback() throws Exception {
        AtomicInteger calls=new AtomicInteger();
        var server=HttpServer.create(new InetSocketAddress("127.0.0.1",0),0);
        server.createContext("/oauth2/token", request -> {
            String body=new String(request.getRequestBody().readAllBytes(),StandardCharsets.UTF_8);
            assertTrue(body.contains("grant_type=client_credentials")); assertTrue(body.contains("client_secret=secret-ci"));
            calls.incrementAndGet(); var response="{\"access_token\":\"secret-access\",\"expires_in\":60,\"token_type\":\"Bearer\"}".getBytes(StandardCharsets.UTF_8);
            request.sendResponseHeaders(200,response.length); request.getResponseBody().write(response); request.close();
        }); server.start();
        try {
            var resolver=new EnvironmentCredentialResolver(Map.of("TPF_OAUTH_CI_ISSUER","http://127.0.0.1:"+server.getAddress().getPort(),
                    "TPF_OAUTH_CI_CLIENT_ID","client_ci","TPF_OAUTH_CI_CLIENT_SECRET","secret-ci","TPF_CREDENTIAL_DIRECTORY",temporary.resolve("unused").toString()));
            assertTrue(resolver.resolve(new CredentialReference("oauth-client:ci")).isPresent());
            assertTrue(resolver.resolve(new CredentialReference("oauth-client:ci")).isPresent());
            assertEquals(2,calls.get()); assertFalse(Files.exists(temporary.resolve("unused")));
            assertThrows(IllegalStateException.class, () -> new EnvironmentCredentialResolver(Map.of()).resolve(new CredentialReference("oauth-client:ci")));
        } finally { server.stop(0); }
    }
    @Test void refusesInsecureIssuerAndNeverEchoesOAuthErrorBody() {
        assertThrows(IllegalArgumentException.class, () -> new OAuthClient(URI.create("http://auth.example")));
        assertThrows(IllegalArgumentException.class, () -> new OAuthClient(URI.create("https://user:secret@auth.example")));
    }
    @Test void transientRefreshFailurePreservesRotatingCredentialWithoutEchoingResponse() throws Exception {
        var server=HttpServer.create(new InetSocketAddress("127.0.0.1",0),0);
        server.createContext("/oauth2/token", request -> {
            request.getRequestBody().readAllBytes();
            byte[] response="secret-provider-outage".getBytes(StandardCharsets.UTF_8);
            request.sendResponseHeaders(503,response.length); request.getResponseBody().write(response); request.close();
        }); server.start();
        try {
            var store=new UserCredentialDirectory(temporary.resolve("credentials"));
            store.save("human",URI.create("http://127.0.0.1:"+server.getAddress().getPort()),"client_public",tokens("access","refresh",1));
            String before=Files.readString(temporary.resolve("credentials/human.json"));
            var failure=assertThrows(OAuthClient.OAuthFailure.class, () -> store.resolve("human"));
            assertFalse(failure.getMessage().contains("secret-provider-outage"));
            assertEquals(before,Files.readString(temporary.resolve("credentials/human.json")));
        } finally { server.stop(0); }
    }
}
