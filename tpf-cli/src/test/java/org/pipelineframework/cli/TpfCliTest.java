package org.pipelineframework.cli;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.PrintWriter;
import java.io.StringWriter;
import org.junit.jupiter.api.Test;
import picocli.CommandLine;

class TpfCliTest {
    @Test
    void helpWorksAtEveryCommandLevelWithoutInputs() {
        for (String path : java.util.List.of("", "release", "release verify", "deploy", "auth", "auth login", "auth status", "auth logout")) {
            for (String help : java.util.List.of("--help", "-h")) {
                var output = new StringWriter();
                var errors = new StringWriter();
                var command = new CommandLine(new TpfCli()).setOut(new PrintWriter(output, true)).setErr(new PrintWriter(errors, true));
                var arguments = new java.util.ArrayList<String>();
                if (!path.isEmpty()) arguments.addAll(java.util.List.of(path.split(" ")));
                arguments.add(help);
                assertEquals(0, command.execute(arguments.toArray(String[]::new)), path);
                assertTrue(output.toString().contains("Usage: tpf" + (path.isEmpty() ? "" : " " + path)), output.toString());
                assertEquals("", errors.toString());
            }
        }
    }

    @Test
    void reportsAStableVersionString() {
        StringWriter output = new StringWriter();
        CommandLine command = new CommandLine(new TpfCli()).setOut(new PrintWriter(output, true));

        assertEquals(0, command.execute("--version"));
        assertTrue(output.toString().startsWith("tpf "));
    }

    @Test
    void invalidOutputFailsBeforeReleaseResolutionOrDeployment() {
        StringWriter errors = new StringWriter();
        CommandLine command = new CommandLine(new TpfCli()).setErr(new PrintWriter(errors, true));

        assertEquals(2, command.execute("release", "verify", "--output", "xml"));
        assertTrue(errors.toString().contains("--output must be human or json"));

        errors.getBuffer().setLength(0);
        assertEquals(2, command.execute("deploy", "production", "--output", "xml"));
        assertTrue(errors.toString().contains("--output must be human or json"));
    }
}
