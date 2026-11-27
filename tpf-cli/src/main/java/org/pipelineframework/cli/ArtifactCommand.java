package org.pipelineframework.cli;

import picocli.CommandLine.Command;
import picocli.CommandLine.Model.CommandSpec;
import picocli.CommandLine.Spec;

@Command(
    mixinStandardHelpOptions = true,
    name = "artifact",
    description = "Upload immutable pipeline artifacts via TPF Cloud presigned intake.",
    subcommands = {ArtifactUploadCommand.class})
final class ArtifactCommand implements Runnable {
    @Spec private CommandSpec spec;

    @Override
    public void run() {
        spec.commandLine().usage(spec.commandLine().getOut());
    }
}
