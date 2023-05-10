package com.asml.apa.wta.core.utils;

import com.asml.apa.wta.core.config.RuntimeConfig;
import java.io.IOException;
import org.apache.kafka.common.KafkaException;

/**
 * Common functionalities between different data sources such as spark and flink.
 *
 * @author Henry Page
 * @since 1.0.0
 */
public interface CollectorInterface {

  /**
   * Reads the config file, and processes any required information from config file.
   * This method is meant to be overrided as the base implementation merely reads the config and returns it.
   *
   * @return The associated config object
   */
  default RuntimeConfig processConfig() {
    return WtaUtils.readConfig();
  }

  /**
   * Should be called on plugin shutdown to ensure that parquet is valid.
   *
   * @param config The config file to get the path where the validation script is located.
   * @throws IllegalStateException if the parquet is invalid, (according to WTA rules)
   */
  void validateFormat(RuntimeConfig config) throws IllegalStateException;

  /**
   * Initializes logger at start of application.
   *
   * @param config The config file to get Log4J properties.
   * @throws IOException if the logger cannot be initialized
   */
  void initializeLogger(RuntimeConfig config) throws IOException;

  /**
   * Closes the logger resource.
   *
   * @throws IOException if the logger cannot be closed
   */
  void flushAndCloseLogger() throws IOException;

  /**
   * Extracts the kafka config and tests the connection if necessary.
   *
   * @param config The config to extract kafka broker url
   * @throws KafkaException If a connection can't be established
   */
  void configureKafka(RuntimeConfig config) throws KafkaException;

  /**
   * Extracts the arrow config.
   *
   * @param config The config to extract
   * @throws IllegalArgumentException if arrow can not be initialised
   */
  void configureArrow(RuntimeConfig config) throws IllegalArgumentException;
}
