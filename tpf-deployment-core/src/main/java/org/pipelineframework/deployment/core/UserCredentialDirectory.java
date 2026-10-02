package org.pipelineframework.deployment.core;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.net.URI;
import java.nio.channels.FileChannel;
import java.nio.file.*;
import java.nio.file.attribute.PosixFilePermissions;
import java.time.Instant;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;
import org.pipelineframework.deployment.release.Credential;

/** Refresh credentials live only here; atomic rotation is serialized across threads/processes. */
public final class UserCredentialDirectory {
    private static final ObjectMapper JSON = new ObjectMapper();
    private final Path directory;
    public UserCredentialDirectory(Path directory) { this.directory = directory.toAbsolutePath().normalize(); }
    public static Path configured(Map<String,String> environment) {
        String value = environment.get("TPF_CREDENTIAL_DIRECTORY");
        return value == null || value.isBlank() ? Path.of(System.getProperty("user.home"), ".tpf", "credentials") : Path.of(value);
    }
    private Path file(String profile) {
        if (!profile.matches("[A-Za-z0-9][A-Za-z0-9_-]{0,63}")) throw new IllegalArgumentException("Invalid OAuth profile name");
        return directory.resolve(profile + ".json");
    }
    private void secure() throws Exception {
        for (Path p=directory; p!=null; p=p.getParent()) if (Files.isSymbolicLink(p)) throw new IllegalStateException("Symlinked credential path refused");
        if (directory.equals(Path.of(System.getProperty("user.home")).toAbsolutePath()) || directory.getParent() == null)
            throw new IllegalStateException("Use a dedicated credential directory");
        Files.createDirectories(directory, PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString("rwx------")));
        Files.setPosixFilePermissions(directory, PosixFilePermissions.fromString("rwx------"));
    }
    private void verifyFile(Path path) throws Exception {
        if (Files.isSymbolicLink(path) || !Files.isRegularFile(path, LinkOption.NOFOLLOW_LINKS)
                || !Files.getPosixFilePermissions(path).equals(PosixFilePermissions.fromString("rw-------")))
            throw new IllegalStateException("Credential file must be a regular owner-only file");
    }
    private <T> T locked(String profile, Supplier<T> action) {
        synchronized (UserCredentialDirectory.class) {
            try {
                secure();
                Path lockPath = directory.resolve(profile + ".lock"); file(profile);
                if (Files.isSymbolicLink(lockPath)) throw new IllegalStateException("Credential lock symlink refused");
                if (!Files.exists(lockPath)) Files.createFile(lockPath, PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString("rw-------")));
                verifyFile(lockPath);
                try (var channel = FileChannel.open(lockPath, StandardOpenOption.WRITE, LinkOption.NOFOLLOW_LINKS);
                     var ignored = channel.lock()) { return action.get(); }
            } catch (OAuthClient.OAuthFailure safe) { throw safe; }
            catch (Exception failure) { throw new IllegalStateException("Credential directory is unavailable or insecure; check its mount and owner-only permissions", failure); }
        }
    }
    public void save(String profile, URI issuer, String clientId, JsonNode tokens) {
        locked(profile, () -> { write(profile, issuer, clientId, tokens); return null; });
    }
    private void write(String profile, URI issuer, String clientId, JsonNode tokens) {
        Path temporary = null;
        try {
            long ttl = tokens.path("expires_in").asLong(0);
            if (ttl <= 0 || !"Bearer".equalsIgnoreCase(tokens.path("token_type").asText())) throw new OAuthClient.OAuthFailure("invalid_response");
            var value = JSON.createObjectNode();
            value.put("issuer", issuer.toString()); value.put("clientId", clientId);
            value.put("accessToken", OAuthClient.text(tokens, "access_token"));
            value.put("refreshToken", OAuthClient.text(tokens, "refresh_token"));
            value.put("expiresAt", Math.addExact(Instant.now().getEpochSecond(), ttl));
            Path target = file(profile);
            if (Files.exists(target, LinkOption.NOFOLLOW_LINKS)) verifyFile(target);
            temporary = Files.createTempFile(directory, "rotation-", ".tmp", PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString("rw-------")));
            Files.write(temporary, JSON.writeValueAsBytes(value));
            Files.move(temporary, target, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
        } catch (OAuthClient.OAuthFailure safe) { throw safe; }
        catch (Exception failure) { throw new IllegalStateException("Credential rotation could not be stored safely", failure); }
        finally { if (temporary != null) try { Files.deleteIfExists(temporary); } catch (Exception ignored) {} }
    }
    public Optional<Credential> resolve(String profile) {
        return locked(profile, () -> {
            Path path = file(profile);
            if (!Files.exists(path, LinkOption.NOFOLLOW_LINKS)) return Optional.empty();
            try {
                verifyFile(path); JsonNode value = JSON.readTree(Files.readAllBytes(path));
                if (value.path("expiresAt").asLong() <= Instant.now().plusSeconds(30).getEpochSecond()) {
                    var issuer = URI.create(OAuthClient.text(value, "issuer"));
                    var clientId = OAuthClient.text(value, "clientId");
                    try {
                        var tokens = new OAuthClient(issuer).request("token", Map.of("grant_type", "refresh_token", "client_id", clientId,
                                "refresh_token", OAuthClient.text(value, "refreshToken")));
                        write(profile, issuer, clientId, tokens);
                        value = JSON.readTree(Files.readAllBytes(path));
                    } catch (OAuthClient.OAuthFailure failure) {
                        if (failure.code.equals("invalid_grant")) Files.delete(path);
                        throw failure;
                    }
                }
                return Optional.of(new Credential.Bearer(OAuthClient.text(value, "accessToken")));
            } catch (OAuthClient.OAuthFailure safe) { throw safe; }
            catch (Exception failure) { throw new IllegalStateException("Credentials are invalid or inaccessible; run tpf auth login", failure); }
        });
    }
    public void logout(String profile) {
        locked(profile, () -> {
            try { Path path = file(profile); if (Files.exists(path, LinkOption.NOFOLLOW_LINKS)) { verifyFile(path); Files.delete(path); } return null; }
            catch (Exception failure) { throw new IllegalStateException("Credentials could not be removed safely", failure); }
        });
    }
}
