package com.asml.apa.wta.core.model;

import com.asml.apa.wta.core.model.enums.Domain;
import lombok.AccessLevel;
import lombok.Builder;
import lombok.Data;
import lombok.Getter;

/**
 * Workload class corresponding to WTA format.
 *
 * @author Lohithsai Yadala Chanchu
 * @author Atour Mousavi Gourabi
 * @since 1.0.0
 */
@Data
@Builder
public class Workload implements BaseTraceObject {
  private static final long serialVersionUID = -4547341610378381743L;

  @Getter(value = AccessLevel.NONE)
  private final String schema_version = this.getSchemaVersion();

  private final Workflow[] workflows;

  private final long total_workflows;

  private final long total_tasks;

  private final Domain domain;

  private final long date_start;

  private final long date_end;

  private final long num_sites;

  private final long num_resources;

  private final long num_users;

  private final long num_groups;

  private final double total_resource_seconds;

  private final String[] authors;

  @Builder.Default
  private final double min_resource_task = -1.0;

  @Builder.Default
  private final double max_resource_task = -1.0;

  @Builder.Default
  private final double std_resource_task = -1.0;

  @Builder.Default
  private final double mean_resource_task = -1.0;

  @Builder.Default
  private final double median_resource_task = -1.0;

  @Builder.Default
  private final double first_quartile_resource_task = -1.0;

  @Builder.Default
  private final double third_quartile_resource_task = -1.0;

  @Builder.Default
  private final double cov_resource_task = -1.0;

  @Builder.Default
  private final double min_memory = -1.0;

  @Builder.Default
  private final double max_memory = -1.0;

  @Builder.Default
  private final double std_memory = -1.0;

  @Builder.Default
  private final double mean_memory = -1.0;

  @Builder.Default
  private final double median_memory = -1.0;

  @Builder.Default
  private final long first_quartile_memory = -1L;

  @Builder.Default
  private final long third_quartile_memory = -1L;

  @Builder.Default
  private final double cov_memory = -1.0;

  @Builder.Default
  private final long min_network_io_time = -1L;

  @Builder.Default
  private final long max_network_io_time = -1L;

  @Builder.Default
  private final double std_network_io_time = -1.0;

  @Builder.Default
  private final double mean_network_io_time = -1.0;

  @Builder.Default
  private final double median_network_io_time = -1.0;

  @Builder.Default
  private final long first_quartile_network_io_time = -1L;

  @Builder.Default
  private final long third_quartile_network_io_time = -1L;

  @Builder.Default
  private final double cov_network_io_time = -1.0;

  @Builder.Default
  private final double min_disk_space_usage = -1.0;

  @Builder.Default
  private final double max_disk_space_usage = -1.0;

  @Builder.Default
  private final double std_disk_space_usage = -1.0;

  @Builder.Default
  private final double mean_disk_space_usage = -1.0;

  @Builder.Default
  private final long median_disk_space_usage = -1L;

  @Builder.Default
  private final long first_quartile_disk_space_usage = -1L;

  @Builder.Default
  private final long third_quartile_disk_space_usage = -1L;

  @Builder.Default
  private final double cov_disk_space_usage = -1.0;

  @Builder.Default
  private final int min_energy = -1;

  @Builder.Default
  private final int max_energy = -1;

  @Builder.Default
  private final double std_energy = -1.0;

  @Builder.Default
  private final double mean_energy = -1.0;

  @Builder.Default
  private final int median_energy = -1;

  @Builder.Default
  private final int first_quartile_energy = -1;

  @Builder.Default
  private final int third_quartile_energy = -1;

  @Builder.Default
  private final double cov_energy = -1.0;

  private final String workload_description;
}
