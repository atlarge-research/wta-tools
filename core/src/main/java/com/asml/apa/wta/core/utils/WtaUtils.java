package com.asml.apa.wta.core.utils;

import com.asml.apa.wta.core.config.RuntimeConfig;
import com.asml.apa.wta.core.model.enums.Domain;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.FileInputStream;
import java.util.HashMap;
import java.util.Map;

/**
 * Utility class for WTA.
 *
 * @author Henry Page
 * @author Lohithsai Yadala Chanchu
 * @author Atour Mousavi Gourabi
 * @since 1.0.0
 */
public class WtaUtils {

  private static final String CONFIG_DIR = "config.json";

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
    var configBuilder = RuntimeConfig.builder();

    try (FileInputStream fis = new FileInputStream(configDir)) {
      ObjectMapper mapper = new ObjectMapper();

      JsonNode rootNode = mapper.readTree(fis);

      JsonNode workloadNode = rootNode.get("workloadSettings");
      JsonNode resourceNode = rootNode.get("resourceSettings");
      JsonNode logNode = rootNode.get("logSettings");
      JsonNode sparkMetricsNode = rootNode.get("sparkMetricsLevel");

      String[] authors = workloadNode.get("author").asText().split("\\s*,\\s*");

      Domain domain = Domain.extractAsEnum(workloadNode.get("domain").asText());

      String description = workloadNode.has("description")
          ? workloadNode.get("description").asText()
          : "";
      Map<String, String> events = resourceNode.has("events")
          ? mapper.convertValue(resourceNode.get("events"), new TypeReference<>() {})
          : new HashMap<>();

      String logLevel = logNode.has("logLevel") ? logNode.get("logLevel").asText() : "ERROR";

      boolean sparkMetricsLevel = sparkMetricsNode.has("isStageLevel")
          ? sparkMetricsNode.get("isStageLevel").asBoolean()
          : false;

      configBuilder = configBuilder
          .authors(authors)
          .domain(domain)
          .description(description)
          .events(events)
          .logLevel(logLevel)
          .stageLevel(sparkMetricsLevel);
    } catch (EnumConstantNotPresentException e) {
      throw new IllegalArgumentException(e.constantName()
          + " is not a valid domain. It must be BIOMEDICAL, ENGINEERING, INDUSTRIAL, or SCIENTIFIC.");
    } catch (Exception e) {
      throw new IllegalArgumentException(
          "The config file has missing/invalid fields or no config file was found");
    }
    return configBuilder.build();
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
