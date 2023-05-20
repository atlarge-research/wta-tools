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

      String author = workloadNode.get("author").asText();

      Domain domain = Domain.extractAsEnum(workloadNode.get("domain").asText());

      String description = workloadNode.has("description")
          ? workloadNode.get("description").asText()
          : "";
      Map<String, String> events = resourceNode.has("events")
          ? mapper.convertValue(resourceNode.get("events"), new TypeReference<>() {})
          : new HashMap<>();
      configBuilder = configBuilder
          .authors(author)
          .domain(domain)
          .description(description)
          .events(events);
    } catch (EnumConstantNotPresentException e) {
      throw new IllegalArgumentException(e.constantName()
          + " is not a valid domain. It must be one of BIOMEDICAL, ENGINEERING, INDUSTRIAL, SCIENTIFIC.");
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
