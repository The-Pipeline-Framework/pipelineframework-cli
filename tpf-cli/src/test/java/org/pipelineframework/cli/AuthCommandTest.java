package org.pipelineframework.cli;

import com.sun.net.httpserver.HttpServer;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import picocli.CommandLine;
import static org.junit.jupiter.api.Assertions.*;

class AuthCommandTest {
    @TempDir Path temporary;
    @org.junit.jupiter.api.BeforeEach void canonicalTemporaryRoot() throws Exception { temporary=temporary.toRealPath(); }
    private Result execute(String... arguments) {
        var output=new StringWriter(); var error=new StringWriter();
        var command=new CommandLine(new TpfCli()).setOut(new PrintWriter(output)).setErr(new PrintWriter(error));
        int exit=command.execute(arguments); return new Result(exit, output.toString()+error);
    }
    @Test void missingStatusAndRepeatedLogoutAreActionableWithoutSecrets() {
        var status=execute("auth","status","--credential-dir",temporary.resolve("credentials").toString());
        assertEquals(5,status.exit()); assertTrue(status.output().contains("tpf auth login"));
        assertEquals(0,execute("auth","logout","--credential-dir",temporary.resolve("credentials").toString()).exit());
        assertEquals(0,execute("auth","logout","--credential-dir",temporary.resolve("credentials").toString()).exit());
    }
    @Test void helpWorksWithoutCredentialsOrRequiredLoginArguments() {
        assertEquals(0,execute("auth","--help").exit());
        for (String command : java.util.List.of("login","status","logout"))
            assertEquals(0,execute("auth",command,"--help").exit());
    }
    @Test void deviceLoginPendingSuccessStatusAndLogoutNeverPrintTokens() throws Exception {
        try (var fixture=new DeviceFixture("pending")) {
            var result=login(fixture);
            assertEquals(0,result.exit()); assertTrue(result.output().contains("USER-CODE"));
            assertFalse(result.output().contains("secret-access")); assertFalse(result.output().contains("secret-refresh"));
            assertFalse(result.output().contains("secret-device"));
            assertTrue(Files.exists(temporary.resolve("credentials/cloud.json")));
            assertEquals(0,execute("auth","status","--credential-dir",temporary.resolve("credentials").toString()).exit());
            assertEquals(0,execute("auth","logout","--credential-dir",temporary.resolve("credentials").toString()).exit());
            assertFalse(Files.exists(temporary.resolve("credentials/cloud.json")));
        }
    }
    @Test void deviceDenialExpiryAndProviderErrorNeverPersistOrEchoSecretBodies() throws Exception {
        for (String mode : java.util.List.of("expired_token","access_denied","unknown")) {
            try (var fixture=new DeviceFixture(mode)) {
                var result=login(fixture); assertEquals(5,result.exit());
                assertFalse(result.output().contains("secret-error"));
                assertFalse(Files.exists(temporary.resolve("credentials/cloud.json")));
            }
        }
    }
    @Test void respectsSlowDownBeforeSuccess() throws Exception {
        try (var fixture=new DeviceFixture("slow_down")) {
            long started=System.nanoTime(); assertEquals(0,login(fixture).exit());
            assertTrue(java.time.Duration.ofNanos(System.nanoTime()-started).toSeconds()>=7);
        }
    }
    @Test void localDeviceDeadlineStopsPollingWithoutPersistingCredentials() throws Exception {
        try (var fixture=new DeviceFixture("local-expiry")) {
            assertEquals(5,login(fixture).exit());
            assertFalse(Files.exists(temporary.resolve("credentials/cloud.json")));
        }
    }
    @Test void verificationHostRequiresExplicitTrustAndRetainsTransportAndUserInfoChecks() throws Exception {
        try (var fixture=new DeviceFixture("success", "https://login.example/activate")) {
            assertEquals(5,login(fixture).exit());
            assertEquals(0,login(fixture,"LOGIN.EXAMPLE").exit());
            assertEquals(0,execute("auth","logout","--credential-dir",temporary.resolve("credentials").toString()).exit());
        }
        for (String uri : java.util.List.of("https://unexpected.example/activate",
                "https://login.example.evil/activate", "http://login.example/activate",
                "https://user:secret@login.example/activate")) {
            try (var fixture=new DeviceFixture("success",uri)) {
                var result=login(fixture,"login.example");
                assertEquals(5,result.exit());
                assertFalse(result.output().contains("Open "));
                assertFalse(Files.exists(temporary.resolve("credentials/cloud.json")));
            }
        }
    }
    @Test void retainedFilesystemCausesDoNotReachHumanOutput() throws Exception {
        var blocked=temporary.resolve("secret-file-name"); Files.createFile(blocked);
        var result=execute("auth","status","--credential-dir",blocked.toString());
        assertEquals(5,result.exit());
        assertFalse(result.output().contains("secret-file-name"));
    }
    private Result login(DeviceFixture fixture, String... trustedHosts) {
        var arguments=new java.util.ArrayList<>(java.util.List.of("auth","login","--issuer",fixture.issuer,
                "--client-id","client_public", "--credential-dir",temporary.resolve("credentials").toString()));
        for (String host : trustedHosts) { arguments.add("--verification-host"); arguments.add(host); }
        return execute(arguments.toArray(String[]::new));
    }
    private record Result(int exit, String output) {}
    private static class DeviceFixture implements AutoCloseable {
        final HttpServer server; final String issuer;
        DeviceFixture(String mode) throws Exception {
            this(mode,null);
        }
        DeviceFixture(String mode, String verificationUri) throws Exception {
            server=HttpServer.create(new InetSocketAddress("127.0.0.1",0),0);
            issuer="http://127.0.0.1:"+server.getAddress().getPort();
            AtomicInteger count=new AtomicInteger();
            server.createContext("/oauth2/device_authorization", request -> {
                String body=new String(request.getRequestBody().readAllBytes(),StandardCharsets.UTF_8);
                assertTrue(body.contains("client_id=client_public")); assertFalse(body.contains("client_secret"));
                send(request,200,"{\"device_code\":\"secret-device\",\"user_code\":\"USER-CODE\",\"verification_uri\":\""+(verificationUri==null ? issuer+"/activate" : verificationUri)+"\",\"expires_in\":"+(mode.equals("local-expiry") ? 1 : 30)+",\"interval\":1}");
            });
            server.createContext("/oauth2/token", request -> {
                String body=new String(request.getRequestBody().readAllBytes(),StandardCharsets.UTF_8);
                assertTrue(body.contains("device_code=secret-device")); assertFalse(body.contains("client_secret"));
                int attempt=count.incrementAndGet();
                if (mode.equals("success") || (mode.equals("pending") || mode.equals("slow_down")) && attempt>1)
                    send(request,200,"{\"access_token\":\"secret-access\",\"refresh_token\":\"secret-refresh\",\"expires_in\":600,\"token_type\":\"Bearer\"}");
                else send(request,400,"{\"error\":\""+(mode.equals("pending") ? "authorization_pending" : mode)+"\",\"error_description\":\"secret-error\"}");
            }); server.start();
        }
        private static void send(com.sun.net.httpserver.HttpExchange request,int status,String body) throws java.io.IOException {
            byte[] bytes=body.getBytes(StandardCharsets.UTF_8); request.sendResponseHeaders(status,bytes.length); request.getResponseBody().write(bytes); request.close();
        }
        @Override public void close() { server.stop(0); }
    }
}
