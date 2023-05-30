package com.asml.apa.wta.core.supplier;

import com.asml.apa.wta.core.dto.OsInfoDto;
import com.sun.management.OperatingSystemMXBean;
import java.lang.management.ManagementFactory;
import java.util.concurrent.CompletableFuture;

/**
 * Supplier that uses Java's operating system MBean to gain access to resource information.
 *
 * @author Atour Mousavi Gourabi
 * @since 1.0.0
 */
public class OperatingSystemSupplier implements InformationSupplier<OsInfoDto> {

  private final OperatingSystemMXBean bean;

  /**
   * Constructs the Supplier.
   *
   * @author Atour Mousavi Gourabi
   * @since 1.0.0
   */
  public OperatingSystemSupplier() {
    bean = (OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean();
  }

  /**
   * Verifies that the supplier is available.
   *
   * @return a {@code boolean} indicating the validity of this supplier
   * @author Atour Mousavi Gourabi
   * @since 1.0.0
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
   * @author Atour Mousavi Gourabi
   * @since 1.0.0
   */
  public long getCommittedVirtualMemorySize() {
    return bean.getCommittedVirtualMemorySize();
  }

  /**
   * Retrieves the amount of free physical memory in bytes.
   *
   * @implNote deprecation suppress warnings is enabled as the bean method
   * was only deprecated in Java 14 and this project is currently in Java 11.
   *
   * @return the amount of free physical memory in bytes
   * @author Atour Mousavi Gourabi
   * @since 1.0.0
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
   * @author Atour Mousavi Gourabi
   * @since 1.0.0
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
   * @author Atour Mousavi Gourabi
   * @since 1.0.0
   */
  public long getProcessCpuTime() {
    return bean.getProcessCpuTime();
  }

  /**
   * Retrieves the amount of total physical memory in bytes.
   *
   * @implNote deprecation suppress warnings is enabled as the bean method
   * was only deprecated in Java 14 and this project is currently in Java 11.
   *
   * @return the amount of total physical memory in bytes
   * @author Atour Mousavi Gourabi
   * @since 1.0.0
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
   * @author Atour Mousavi Gourabi
   * @since 1.0.0
   */
  public double getSystemLoadAverage() {
    return bean.getSystemLoadAverage();
  }

  /**
   * Retrieves the number of processors available to the JVM.
   * The value returned may change during a JVM run, it will never be smaller than 1.
   *
   * @return the number of processors available, never smaller than 1
   * @author Atour Mousavi Gourabi
   * @since 1.0.0
   */
  public int getAvailableProcessors() {
    return bean.getAvailableProcessors();
  }

  /**
   * Gathers the metrics the supplier provides (computed asynchronously).
   *
   * @return an {@link OsInfoDto} containing the gathered metrics
   * @author Atour Mousavi Gourabi
   * @since 1.0.0
   */
  @Override
  public CompletableFuture<OsInfoDto> getSnapshot() {
    if (!isAvailable()) {
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
      return new OsInfoDto(
          vMemSize, freeMemSize, cpuLoad, cpuTime, totalMemSize, availableProc, systemLoadAverage);
    });
  }
}
