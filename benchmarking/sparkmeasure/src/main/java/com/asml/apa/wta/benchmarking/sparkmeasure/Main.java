package com.asml.apa.wta.benchmarking.sparkmeasure;

import org.openjdk.jmh.profile.GCProfiler;
import org.openjdk.jmh.profile.StackProfiler;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

/**
 * Entry point for running the Spark benchmark.
 *
 * @author Pil Kyu CHo
 */
public class Main {

  public static void main(String[] args) throws Exception {
    Options options = new OptionsBuilder()
        .include(SparkBenchmark.class.getSimpleName())
        .addProfiler(StackProfiler.class)
        .addProfiler(GCProfiler.class)
        .build();
    new Runner(options).run();
  }
}
