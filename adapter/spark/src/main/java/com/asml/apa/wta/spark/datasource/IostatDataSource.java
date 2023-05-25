package com.asml.apa.wta.spark.datasource;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import com.asml.apa.wta.spark.datasource.dto.IostatDataSourceDto;
import lombok.Data;

@Data
public class IostatDataSource {

    private double tps;
    private double KBReadPerSec;
    private double KBWrtnPerSec;
    private double KBDscdPerSec;
    private double KBRead;
    private double KBWrtn;
    private double KBDscd;
    private String executorId;


    public IostatDataSource() throws IOException, InterruptedException {
    }

    public IostatDataSource getAllMetrics() throws IOException, InterruptedException {
        this.tps = this.executeCommand("iostat -d | awk '$1 == \"sdc\"' | awk '{print $2}'");
        this.KBReadPerSec = this.executeCommand("iostat -d | awk '$1 == \"sdc\"' | awk '{print $3}'");
        this.KBWrtnPerSec = this.executeCommand("iostat -d | awk '$1 == \"sdc\"' | awk '{print $4}'");
        this.KBDscdPerSec = this.executeCommand("iostat -d | awk '$1 == \"sdc\"' | awk '{print $5}'");
        this.KBRead = this.executeCommand("iostat -d | awk '$1 == \"sdc\"' | awk '{print $6}'");
        this.KBWrtn = this.executeCommand("iostat -d | awk '$1 == \"sdc\"' | awk '{print $7}'");
        this.KBDscd = this.executeCommand("iostat -d | awk '$1 == \"sdc\"' | awk '{print $8}'");
        return this;
    }

    public double executeCommand(String command) throws InterruptedException, IOException {
        // Execute the Bash command on each partition of the RDD and collect the output
        String[] commands = {"bash", "-c", command};
        Process process = Runtime.getRuntime().exec(commands);
        process.waitFor();

        BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()));
        String line = reader.readLine();
        reader.close();

        // Print the output from each executor
        double a = Double.parseDouble(line);

        return a;
    }
    public IostatDataSourceDto getIostatDto(String executorId) {
        return IostatDataSourceDto.builder().tps(getTps()).KBReadPerSec(getKBReadPerSec())
                .KBWrtnPerSec(getKBWrtnPerSec()).KBDscdPerSec(getKBDscdPerSec()).KBRead(getKBRead())
                .KBWrtn(getKBWrtn()).KBDscd(getKBDscd()).executorId((executorId)).build();
    }
}
