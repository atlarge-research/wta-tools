package com.asml.apa.wta.core.datasource;

import com.sun.management.OperatingSystemMXBean;
import java.lang.management.ManagementFactory;

/**
 * Data source that uses Java's operating system MBean to gain access to mainly resource information.
 *
 * @author Atour Mousavi Gourabi
 * @since 1.0.0
 */
public abstract class OperatingSystemDataSource {

  private final OperatingSystemMXBean bean;

  /**
   * Constructs the data source.
   *
   * @author Atour Mousavi Gourabi
   * @since 1.0.0
   */
  public OperatingSystemDataSource() {
    bean = (OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean();
  }

  /**
   * Verifies the validity of the data source.
   *
   * @return a {@code boolean} indicating the validity of this data source
   * @author Atour Mousavi Gourabi
   * @since 1.0.0
   */
  public boolean isValid() {
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
}
