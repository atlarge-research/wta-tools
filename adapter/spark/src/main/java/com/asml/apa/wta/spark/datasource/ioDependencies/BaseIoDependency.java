package com.asml.apa.wta.spark.datasource.ioDependencies;

import lombok.extern.slf4j.Slf4j;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.concurrent.CompletableFuture;

@Slf4j
public class BaseIoDependency {
    /**
     * Executes given bash command and returns the terminal output.
     *
     * @param command The bash command string that is run.
     * @return CompletableFuture that returns the output of the command
     * @author Lohithsai Yadala Chanchu
     * @since 1.0.0
     */

    public CompletableFuture<String> executeCommand(String command) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                String[] commands = {"bash", "-c", command};
                Process process = new ProcessBuilder(commands).start();
                process.waitFor();

                String line = "";
                try (BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()))) {
                    line = reader.readLine();
                } catch (IOException e) {
                    log.error(
                            "Something went wrong while trying to read bash command outputs. The cause is: {}",
                            e.getCause().toString());
                }

                return line;
            } catch (Exception e) {
                log.error(
                        "Something went wrong while trying to read bash command outputs. The cause is: {}",
                        e.getCause().toString());
                return "";
            }
        });
    }
}
