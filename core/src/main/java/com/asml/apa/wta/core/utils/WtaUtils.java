package com.asml.apa.wta.core.utils;

import com.asml.apa.wta.core.config.RuntimeConfig;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class WtaUtils {

  private static final String CONFIG_DIR = "config.json";
  private static RuntimeConfig config = null;

  /**
   * Utility classes should not have a public or default constructor.
   */
  private WtaUtils() {
    throw new IllegalStateException();
  }

  /**
   * Reads the config file and creates the associated config object.
   *
   * @param configDir The directory where the config is located.
   * @return The associated config object
   */
  public static RuntimeConfig readConfig(String configDir) {
    if (config != null) {
      return config;
    }

    var configBuilder = RuntimeConfig.builder();

    try (FileInputStream fis = new FileInputStream(configDir)) {
      ObjectMapper mapper = new ObjectMapper();

      JsonNode rootNode = mapper.readTree(fis);

      JsonNode workloadNode = rootNode.get("workloadSettings");
      JsonNode resourceNode = rootNode.get("resourceSettings");
      JsonNode logNode = rootNode.get("logSettings");

      String author = workloadNode.get("author").asText();
      String domain = workloadNode.get("domain").asText();
      String description = workloadNode.has("description")
          ? workloadNode.get("description").asText()
          : "";
      Map<String, String> events = resourceNode.has("events")
          ? mapper.convertValue(resourceNode.get("events"), new TypeReference<>() {})
          : new HashMap<>();

      String logLevel = logNode.has("logLevel") ? logNode.get("logLevel").asText() : "ERROR";

      configBuilder = configBuilder
          .author(author)
          .domain(domain)
          .description(description)
          .events(events)
          .logLevel(logLevel);
    } catch (IOException e) {
      throw new IllegalArgumentException(
          "The config file has missing/invalid fields or no config file was found");
    }
    config = configBuilder.build();
    return config;
  }

  /**
   * Reads the config file at the specified directory.
   *
   * @return The config file
   */
  public static RuntimeConfig readConfig() {
    return readConfig(CONFIG_DIR);
  }
}
