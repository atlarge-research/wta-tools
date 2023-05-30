package com.asml.apa.wta.core.utils;

import com.asml.apa.wta.core.config.RuntimeConfig;
import com.google.gson.Gson;
import com.google.gson.JsonParseException;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import lombok.extern.slf4j.Slf4j;

/**
 * Utility class for WTA.
 *
 * @author Pil Kyu CHo
 * @author Henry Page
 * @author Lohithsai Yadala Chanchu
 * @author Atour Mousavi Gourabi
 * @since 1.0.0
 */
@Slf4j
public class WtaUtils {

  private static final String CONFIG_DIR = "config.json";

  /**
   * Utility classes should not have a public or default constructor.
   *
   * @throws IllegalStateException when called
   */
  private WtaUtils() {
    throw new IllegalStateException();
  }

  /**
   * Reads the config file and creates the associated config object.
   *
   * @param configDir The directory where the config is located.
   * @return The associated config object
   * @author Atour Mousavi Gourabi
   */
  public static RuntimeConfig readConfig(String configDir) {
    try (FileReader reader = new FileReader(configDir)) {
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
        log.info("The config file does not include a description, this field is highly recommended.");
      } else if (config.getOutputPath() == null) {
        throw new IllegalArgumentException("The config file does not specify the output path");
      }
      return config;
    } catch (JsonParseException e) {
      log.error("The config file has invalid fields");
      throw new IllegalArgumentException("The config file has invalid fields");
    } catch (FileNotFoundException e) {
      log.error("No config file was found at {}", configDir);
      throw new IllegalArgumentException("No config file was found at " + configDir);
    } catch (IOException e) {
      log.error("Something went wrong while reading {}", configDir);
      throw new IllegalArgumentException("Something went wrong while reading " + configDir);
    }
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
