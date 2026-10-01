package org.pipelineframework.cli;

import picocli.CommandLine.Command;

@Command(name = "release", description = "Inspect immutable TPF Releases.", subcommands = ReleaseVerifyCommand.class)
final class ReleaseCommand implements Runnable {
    @Override public void run() { picocli.CommandLine.usage(this, System.out); }
}
