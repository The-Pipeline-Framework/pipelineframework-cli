package org.pipelineframework.cli;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.PrintWriter;
import java.io.StringWriter;
import org.junit.jupiter.api.Test;
import picocli.CommandLine;

class TpfCliTest {
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
