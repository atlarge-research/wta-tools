package com.asml.apa.wta.core.model;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class WorkloadTest {

  @Test
  void defaultBuilderValues() {
    Workload workload = Workload.builder()
        .authors(new String[] {"authors"})
        .domain(Domain.INDUSTRIAL)
        .build();
    assertThat(workload.getAuthors()).isEqualTo(new String[] {"authors"});
    assertThat(workload.getDomain()).isEqualTo(Domain.INDUSTRIAL);
    assertThat(workload.getWorkloadDescription()).isEqualTo("");
    assertThat(workload.getTotalWorkflows()).isEqualTo(-1L);
    assertThat(workload.getTotalTasks()).isEqualTo(-1L);
    assertThat(workload.getDateStart()).isEqualTo(-1L);
    assertThat(workload.getDateEnd()).isEqualTo(-1L);
    assertThat(workload.getNumSites()).isEqualTo(-1L);
    assertThat(workload.getNumResources()).isEqualTo(-1L);
    assertThat(workload.getNumUsers()).isEqualTo(-1L);
    assertThat(workload.getNumGroups()).isEqualTo(-1L);
    assertThat(workload.getTotalResourceSeconds()).isEqualTo(-1.0);
    assertThat(workload.getMinResourceTask()).isEqualTo(-1.0);
    assertThat(workload.getMaxResourceTask()).isEqualTo(-1.0);
    assertThat(workload.getStdResourceTask()).isEqualTo(-1.0);
    assertThat(workload.getMeanResourceTask()).isEqualTo(-1.0);
    assertThat(workload.getMedianResourceTask()).isEqualTo(-1.0);
    assertThat(workload.getFirstQuartileResourceTask()).isEqualTo(-1.0);
    assertThat(workload.getThirdQuartileResourceTask()).isEqualTo(-1.0);
    assertThat(workload.getCovResourceTask()).isEqualTo(-1.0);
    assertThat(workload.getMinMemory()).isEqualTo(-1.0);
    assertThat(workload.getMaxMemory()).isEqualTo(-1.0);
    assertThat(workload.getStdMemory()).isEqualTo(-1.0);
    assertThat(workload.getMeanMemory()).isEqualTo(-1.0);
    assertThat(workload.getMedianMemory()).isEqualTo(-1.0);
    assertThat(workload.getFirstQuartileMemory()).isEqualTo(-1.0);
    assertThat(workload.getThirdQuartileMemory()).isEqualTo(-1.0);
    assertThat(workload.getCovMemory()).isEqualTo(-1.0);
    assertThat(workload.getMinNetworkUsage()).isEqualTo(-1L);
    assertThat(workload.getMaxNetworkUsage()).isEqualTo(-1L);
    assertThat(workload.getStdNetworkUsage()).isEqualTo(-1.0);
    assertThat(workload.getMeanNetworkUsage()).isEqualTo(-1.0);
    assertThat(workload.getMedianNetworkUsage()).isEqualTo(-1L);
    assertThat(workload.getFirstQuartileNetworkUsage()).isEqualTo(-1L);
    assertThat(workload.getThirdQuartileNetworkUsage()).isEqualTo(-1L);
    assertThat(workload.getCovNetworkUsage()).isEqualTo(-1.0);
    assertThat(workload.getMinDiskSpaceUsage()).isEqualTo(-1.0);
    assertThat(workload.getMaxDiskSpaceUsage()).isEqualTo(-1.0);
    assertThat(workload.getStdDiskSpaceUsage()).isEqualTo(-1.0);
    assertThat(workload.getMeanDiskSpaceUsage()).isEqualTo(-1.0);
    assertThat(workload.getMedianDiskSpaceUsage()).isEqualTo(-1.0);
    assertThat(workload.getFirstQuartileDiskSpaceUsage()).isEqualTo(-1.0);
    assertThat(workload.getThirdQuartileDiskSpaceUsage()).isEqualTo(-1.0);
    assertThat(workload.getCovDiskSpaceUsage()).isEqualTo(-1.0);
    assertThat(workload.getMinEnergy()).isEqualTo(-1.0);
    assertThat(workload.getMaxEnergy()).isEqualTo(-1.0);
    assertThat(workload.getStdEnergy()).isEqualTo(-1.0);
    assertThat(workload.getMeanEnergy()).isEqualTo(-1.0);
    assertThat(workload.getMedianEnergy()).isEqualTo(-1.0);
    assertThat(workload.getFirstQuartileEnergy()).isEqualTo(-1.0);
    assertThat(workload.getThirdQuartileEnergy()).isEqualTo(-1.0);
    assertThat(workload.getCovEnergy()).isEqualTo(-1.0);
  }
}
