package org.pipelineframework.cli;

import java.util.Optional;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.IVersionProvider;

@Command(
    name = "tpf",
    mixinStandardHelpOptions = true,
    versionProvider = TpfCli.ManifestVersionProvider.class,
    description = "Verify and deploy immutable TPF Releases.",
    subcommands = {ReleaseCommand.class, DeployCommand.class})
public final class TpfCli implements Runnable {
    public static final class ManifestVersionProvider implements IVersionProvider {
        @Override
        public String[] getVersion() {
            String version = Optional.ofNullable(TpfCli.class.getPackage().getImplementationVersion())
                .orElse("development");
            return new String[] {"tpf " + version};
        }
    }

    @Override
    public void run() {
        CommandLine.usage(this, System.out);
    }

    public static void main(String[] args) {
        int exitCode = new CommandLine(new TpfCli()).execute(args);
        System.exit(exitCode);
    }
}
