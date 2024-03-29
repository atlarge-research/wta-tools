package com.asml.apa.wta.core.config;

import com.asml.apa.wta.core.model.Domain;
import com.google.gson.Gson;
import com.google.gson.JsonParseException;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

/**
 * Config class for the plugin.
 *
 * @author Henry Page
 * @author Lohithsai Yadala Chanchu
 * @author Pil Kyu Cho
 * @author Atour Mousavi Gourabi
 * @since 1.0.0
 */
@Getter
@Setter
@Builder
@AllArgsConstructor
@NoArgsConstructor
@Slf4j
public class RuntimeConfig {

  private String[] authors;

  private Domain domain;

  @Builder.Default
  private String description = "";

  @Builder.Default
  private boolean isStageLevel = false;

  @Builder.Default
  private int resourcePingInterval = 500;

  @Builder.Default
  private int executorSynchronizationInterval = -1;

  private String outputPath;

  @Builder.Default
  private boolean aggregateMetrics = false;

  /**
   * Reads the config file and creates the associated config object.
   *
   * @param configFile                  filepath to the config file
   * @return                            associated config object
   * @throws IllegalArgumentException   when the config file is invalid
   * @since 1.0.0
   */
  @SuppressWarnings("CyclomaticComplexity")
  public static RuntimeConfig readConfig(String configFile) {
    log.trace("Reading config file from {}.", configFile);
    try (FileReader reader = new FileReader(configFile)) {
      Gson gson = new Gson();
      RuntimeConfig config = gson.fromJson(reader, RuntimeConfig.class);
      if (config.getAuthors() == null || config.getAuthors().length < 1) {
        log.error(
            "The config file does not specify any authors, it is mandatory to specify at least one author.");
        throw new IllegalArgumentException("The config file does not specify any authors");
      } else if (config.getDomain() == null) {
        log.error("The config file does not specify a domain, this field is mandatory.");
        throw new IllegalArgumentException("The config file does not specify a domain");
      } else if (config.getDescription() == null
          || config.getDescription().isBlank()) {
        log.warn("The config file does not include a description, this field is highly recommended.");
      } else if (config.getOutputPath() == null) {
        log.error("The config file does not specify an output path, this field is mandatory.");
        throw new IllegalArgumentException("The config file does not specify the output path");
      } else if (config.getResourcePingInterval() <= 0) {
        log.error("Resource ping interval must be greater than 0.");
        throw new IllegalArgumentException("Resource ping interval must be greater than 0");
      }
      log.trace("Successfully read config file from {}.", configFile);
      return config;
    } catch (JsonParseException e) {
      log.error("The config file has invalid fields.");
      throw new IllegalArgumentException("The config file has invalid fields");
    } catch (FileNotFoundException e) {
      log.error("No config file was found at {}.", configFile);
      throw new IllegalArgumentException("No config file was found at " + configFile);
    } catch (IOException e) {
      log.error("Something went wrong while reading {}.", configFile);
      throw new IllegalArgumentException("Something went wrong while reading " + configFile);
    } catch (Exception e) {
      log.error("\"configFile\" was not set in the command line arguments or system property.");
      throw new IllegalArgumentException(
          "\"configFile\" was not set in the command line arguments or system property");
    }
  }
}
