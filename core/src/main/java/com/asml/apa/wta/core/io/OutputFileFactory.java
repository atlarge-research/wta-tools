package com.asml.apa.wta.core.io;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ServiceLoader;
import lombok.extern.slf4j.Slf4j;

/**
 * Factory for the OutputFile implementations.
 *
 * @author Atour Mousavi Gourabi
 */
@Slf4j
public class OutputFileFactory {

  public OutputFile create(String path) {
    ServiceLoader<OutputFile> serviceLoader = ServiceLoader.load(OutputFile.class);

    for (OutputFile implementation : serviceLoader) {
      try {
        if (!implementation.acceptsLocation(path)) {
          continue;
        }
        implementation.setPath(path);
        return implementation;
      } catch (IOException e) {
        log.error("Could not set OutputFile field for implementation {} and path {}.", implementation, path);
      }
    }

    return new DiskOutputFile(Path.of(path));
  }
}
