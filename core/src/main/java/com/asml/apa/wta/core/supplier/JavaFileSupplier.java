package com.asml.apa.wta.core.supplier;

import com.asml.apa.wta.core.dto.JvmFileDto;
import java.io.File;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import lombok.extern.slf4j.Slf4j;

/**
 * Supplier for Java file system metrics.
 *
 * @author Henry Page
 * @since 1.0.0
 */
@Slf4j
public class JavaFileSupplier implements InformationSupplier<JvmFileDto> {

  private final File rootDir;

  private boolean isAvailable;

  /**
   * Fetches the root directory, and tests for availability.
   */
  public JavaFileSupplier() {
    this.rootDir = new File("/");
    this.isAvailable = isAvailable();
  }

  /**
   * If there is a filesystem which does not have a root directory, this supplier will not be available.
   *
   * @return true iff the directory exists and read perms are available, false otherwise
   */
  @Override
  public boolean isAvailable() {
    try {
      return this.rootDir.isDirectory();
    } catch (SecurityException e) {
      log.debug(
          "The root directory exists, but read permissions were not granted. Disk space metrics will not be collected.",
          e);
      return false;
    }
  }

  /**
   * Gets a snapshot of some disk metrics from the JVM.
   *
   * @return A {@link CompletableFuture} containing the snapshot of metrics
   */
  @Override
  public CompletableFuture<Optional<JvmFileDto>> getSnapshot() {
    if (!isAvailable) {
      return notAvailableResult();
    }

    return CompletableFuture.supplyAsync(() -> {
      try {
        final long totalSpace = rootDir.getTotalSpace();
        final long freeSpace = rootDir.getFreeSpace();
        final long usableSpace = rootDir.getUsableSpace();

        return Optional.of(JvmFileDto.builder()
            .totalSpace(totalSpace)
            .freeSpace(freeSpace)
            .usableSpace(usableSpace)
            .build());
      } catch (Exception e) {
        log.error(
            "Failed to get Java file system metrics, this is likely due to the absent of read permissions.",
            e);
        return Optional.empty();
      }
    });
  }
}
