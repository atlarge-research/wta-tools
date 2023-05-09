package com.asml.apa.wta.core.Utils;

import com.asml.apa.wta.core.Config.RuntimeConfig;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.FileInputStream;

public class WTAUtils {
  private static final String CONFIG_DIR = "config.json";

  /**
   * Reads the config file and creates the associated config object.
   *
   * @param configDir The directory where the config is located.
   * @return The associated config object.
   */
  public static RuntimeConfig readConfig(String configDir) {
    var configBuilder = RuntimeConfig.builder();

    try (FileInputStream fis = new FileInputStream(configDir)) {
      ObjectMapper mapper = new ObjectMapper();

      JsonNode rootNode = mapper.readTree(fis);

      JsonNode workloadNode = rootNode.get("workloadSettings");
      String author = workloadNode.get("author").asText();
      String domain = workloadNode.get("domain").asText();
      String description = workloadNode.has("description")
          ? workloadNode.get("description").asText()
          : "";

      configBuilder = configBuilder.author(author).domain(domain).description(description);

    } catch (Exception e) {
      throw new IllegalArgumentException(
          "The config file has missing/invalid fields or no config file was found");
    }
    return configBuilder.build();
  }

  public static RuntimeConfig readConfig() {
    return readConfig(CONFIG_DIR);
  }
}
