package com.asml.apa.wta.core.utils;

import com.asml.apa.wta.core.config.RuntimeConfig;

/**
 * Common functionalities between different data sources such as spark and flink.
 *
 * @author Henry Page
 * @since 1.0.0
 */
public interface CollectorInterface {

  /**
   * Reads the config file, and processes any required information from config file.
   * This method is meant to be overridden as the base implementation merely reads the config and returns it.
   *
   * @return The associated config object
   */
  default RuntimeConfig processConfig() {
    return WtaUtils.readConfig();
  }
}
