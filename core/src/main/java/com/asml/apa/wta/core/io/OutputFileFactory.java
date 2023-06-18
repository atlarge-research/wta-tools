package com.asml.apa.wta.core.io;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ServiceLoader;
import lombok.extern.slf4j.Slf4j;

/**
 * Factory for the OutputFile implementations.
 *
 * @author Atour Mousavi Gourabi
 * @since 1.0.0
 */
@Slf4j
public class OutputFileFactory {

  /**
   * Create an appropriate {@link OutputFile} implementation for the specified path.
   *
   * @param path a {@link String} representation of the path to write to
   * @return an appropriate {@link OutputFile} implementation for this path when possible
   * @author Atour Mousavi Gourabi
   * @since 1.0.0
   */
  public OutputFile create(String path) {
    ServiceLoader<OutputFile> serviceLoader = ServiceLoader.load(OutputFile.class);

    log.trace("Started loading the OutputFile implementation from the classpath.");

    for (OutputFile implementation : serviceLoader) {
      try {
        if (!implementation.acceptsLocation(path)) {
          log.trace(
              "Implementation {} cannot handle the specified path {}.", implementation.getClass(), path);
          continue;
        }
        implementation.setPath(path);
        log.info("Loaded implementation {} for the specified output path {}.", implementation.getClass(), path);
        return implementation;
      } catch (IOException e) {
        log.error(
            "Could not set OutputFile field for implementation {} and path {}.",
            implementation.getClass(),
            path);
      }
    }

    log.info("Could not find a custom OutputFile implementation in the classpath, defaulting to DiskOutputFile.");

    return new DiskOutputFile(Path.of(path));
  }
}
