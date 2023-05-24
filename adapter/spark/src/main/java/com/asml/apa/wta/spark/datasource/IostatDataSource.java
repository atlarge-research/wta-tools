package com.asml.apa.wta.spark.datasource;

import lombok.Getter;
import lombok.Setter;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.TaskContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.sparkproject.jetty.util.IO;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class IostatDataSource {

    private double tps;
    private double KBReadPerSec;
    private double KBWrtnPerSec;
    private double KBDscdPerSec;
    private double KBRead;
    private double KBWrtn;
    private double KBDscd;
    @Getter
    @Setter
    private Long taskId;

    //should only be in DTO
    @Setter
    @Getter
    private long taskAttemptId = TaskContext.get().taskAttemptId();


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


    public static double getKBDescPerSec() throws IOException, InterruptedException {
        // Execute the Bash command on each partition of the RDD and collect the output
            String[] commands = {"bash", "-c", "iostat -d | awk '$1 == \"sdc\"' | awk '{print $5}'"};
            Process process = Runtime.getRuntime().exec(commands);
            process.waitFor();

            BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()));
            String line = reader.readLine();
            reader.close();

        // Print the output from each executor
        double a = Double.parseDouble(line);

        return a;
    }

    @Override
    public String toString() {
        return "IostatDataSource{" +
                "tps=" + tps +
                ", KBReadPerSec=" + KBReadPerSec +
                ", KBWrtnPerSec=" + KBWrtnPerSec +
                ", KBDscdPerSec=" + KBDscdPerSec +
                ", KBRead=" + KBRead +
                ", KBWrtn=" + KBWrtn +
                ", KBDscd=" + KBDscd +
                ", taskAttemptId=" + taskAttemptId +
                ", taskId=" + taskId +
                '}';
    }
}
