package com.asml.apa.wta.benchmarking.sparkmeasure;

import org.openjdk.jmh.profile.GCProfiler;
import org.openjdk.jmh.profile.StackProfiler;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

public class BenchmarkRunner {

  public static void main(String[] args) throws Exception {
    Options options = new OptionsBuilder()
        .include(SparkSQLBenchmark.class.getSimpleName())
        .addProfiler(StackProfiler.class)
        .addProfiler(GCProfiler.class)
        .build();
    new Runner(options).run();
  }
}
