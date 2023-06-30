package com.asml.apa.wta.core.supplier;

import com.asml.apa.wta.core.dto.OsInfoDto;
import com.sun.management.OperatingSystemMXBean;
import java.lang.management.ManagementFactory;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

/**
 * Supplier that uses Java's operating system MBean to gain access to resource information.
 *
 * @author Atour Mousavi Gourabi
 * @author Henry Page
 * @since 1.0.0
 */
public class OperatingSystemSupplier implements InformationSupplier<OsInfoDto> {

  private final OperatingSystemMXBean bean;

  private boolean isAvailable;

  /**
   * Constructs the Supplier.
   */
  public OperatingSystemSupplier() {
    bean = (OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean();
    this.isAvailable = isAvailable();
  }

  /**
   * Verifies that the supplier is available.
   *
   * @return a {@code boolean} indicating the validity of this supplier
   */
  @Override
  public boolean isAvailable() {
    return bean != null;
  }

  /**
   * Retrieves the amount of virtual memory guaranteed to be available in bytes.
   * If this metric is unavailable it returns -1.
   *
   * @return the amount of virtual memory that is available in bytes, -1 if unavailable
   */
  public long getCommittedVirtualMemorySize() {
    return bean.getCommittedVirtualMemorySize();
  }

  /**
   * Retrieves the amount of free physical memory in bytes.
   *
   * @return the amount of free physical memory in bytes
   */
  @SuppressWarnings("deprecation")
  public long getFreePhysicalMemorySize() {
    return bean.getFreePhysicalMemorySize();
  }

  /**
   * Retrieves the recent CPU usage for the JVM.
   * The value is a double between 0 and 1.
   *
   * @return a value between 0 and 1 indicating the recent CPU usage for the JVM
   */
  public double getProcessCpuLoad() {
    return bean.getProcessCpuLoad();
  }

  /**
   * Retrieves the CPU time for the JVM in nanoseconds.
   * The value is not necessarily with nanosecond accuracy.
   * If this metric is unavailable it returns -1.
   *
   * @return the CPU time used by the JVM in nanoseconds, -1 if unavailable
   */
  public long getProcessCpuTime() {
    return bean.getProcessCpuTime();
  }

  /**
   * Retrieves the amount of total physical memory in bytes.
   *
   * @return the amount of total physical memory in bytes
   */
  @SuppressWarnings("deprecation")
  public long getTotalPhysicalMemorySize() {
    return bean.getTotalPhysicalMemorySize();
  }

  /**
   * Retrieves the system load average for the past minute.
   * If this metric is unavailable it returns some negative value.
   *
   * @return the system load average, negative if unavailable
   */
  public double getSystemLoadAverage() {
    return bean.getSystemLoadAverage();
  }

  /**
   * Retrieves the number of processors available to the JVM.
   * The value returned may change during a JVM run, it will never be smaller than 1.
   *
   * @return the number of processors available, never smaller than 1
   */
  public int getAvailableProcessors() {
    return bean.getAvailableProcessors();
  }

  /**
   * Retrieves the architecture abbreviation.
   *
   * @return The underlying architecture e.g. amd64
   */
  public String getArch() {
    return bean.getArch();
  }

  /**
   * Retrieves the operating system name and version.
   *
   * @return The operating system name and version e.g. Windows 10.0 or Linux 4.4.0-18362-Microsoft
   */
  public String getOperatingSystem() {
    return bean.getName() + " " + bean.getVersion();
  }

  /**
   * Gathers the metrics the supplier provides (computed asynchronously).
   *
   * @return an {@link OsInfoDto} containing the gathered metrics
   */
  @Override
  public CompletableFuture<Optional<OsInfoDto>> getSnapshot() {
    if (!isAvailable) {
      return notAvailableResult();
    }

    return CompletableFuture.supplyAsync(() -> {
      long vMemSize = getCommittedVirtualMemorySize();
      long freeMemSize = getFreePhysicalMemorySize();
      double cpuLoad = getProcessCpuLoad();
      long cpuTime = getProcessCpuTime();
      long totalMemSize = getTotalPhysicalMemorySize();
      int availableProc = getAvailableProcessors();
      double systemLoadAverage = getSystemLoadAverage();
      String architecture = getArch();
      String os = getOperatingSystem();
      return Optional.of(new OsInfoDto(
          vMemSize,
          freeMemSize,
          cpuLoad,
          cpuTime,
          totalMemSize,
          availableProc,
          systemLoadAverage,
          architecture,
          os));
    });
  }
}
