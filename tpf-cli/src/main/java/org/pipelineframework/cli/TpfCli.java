package org.pipelineframework.cli;

import java.io.IOException;
import java.util.Properties;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.IVersionProvider;

@Command(
    name = "tpf",
    mixinStandardHelpOptions = true,
    versionProvider = TpfCli.ManifestVersionProvider.class,
    description = "Verify and deploy immutable TPF Releases.",
    subcommands = {ReleaseCommand.class, DeployCommand.class, AuthCommand.class})
public final class TpfCli implements Runnable {
    public static final class ManifestVersionProvider implements IVersionProvider {
        @Override
        public String[] getVersion() {
            Properties build = new Properties();
            try (var resource = TpfCli.class.getResourceAsStream("/tpf-build.properties")) {
                if (resource == null) throw new IllegalStateException("Missing CLI build version");
                build.load(resource);
            } catch (IOException failure) {
                throw new IllegalStateException("Cannot read CLI build version", failure);
            }
            String version = build.getProperty("version");
            if (version == null || version.isBlank()) throw new IllegalStateException("Missing CLI build version");
            return new String[] {"tpf " + version};
        }
    }

    @CommandLine.Spec private CommandLine.Model.CommandSpec spec;

    @Override
    public void run() {
        spec.commandLine().usage(spec.commandLine().getOut());
    }

    public static void main(String[] args) {
        int exitCode = new CommandLine(new TpfCli()).execute(args);
        System.exit(exitCode);
    }
}
