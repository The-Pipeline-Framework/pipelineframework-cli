package org.pipelineframework.cli;

import picocli.CommandLine.Command;

@Command(mixinStandardHelpOptions = true, name = "release", description = "Inspect immutable TPF Releases.", subcommands = ReleaseVerifyCommand.class)
final class ReleaseCommand implements Runnable {
    @picocli.CommandLine.Spec private picocli.CommandLine.Model.CommandSpec spec;
    @Override public void run() { spec.commandLine().usage(spec.commandLine().getOut()); }
}
