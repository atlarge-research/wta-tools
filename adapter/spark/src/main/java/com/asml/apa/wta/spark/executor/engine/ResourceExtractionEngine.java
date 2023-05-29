package com.asml.apa.wta.spark.executor.engine;

import org.apache.spark.api.plugin.PluginContext;

public class ResourceExtractionEngine {

  private final PluginContext pluginContext;

  public ResourceExtractionEngine(PluginContext pluginContext) {
    this.pluginContext = pluginContext;
  }

  private void initResources() {
    initCommonResources();
    String os = System.getProperty("os.name").toLowerCase();
    if (os.contains("nix") || os.contains("nux") || os.contains("aix")) {
      initUnixResources();
    } else if (os.contains("win")) {
      initWindowsResources();
    }
  }

  private void initCommonResources() {}

  private void initWindowsResources() {}

  private void initUnixResources() {}
}
