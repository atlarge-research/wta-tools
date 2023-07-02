package com.asml.apa.wta.core.model;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class ResourceStateTest {

  @Test
  void defaultBuilderValues() {
    ResourceState resourceState = ResourceState.builder().resourceId(1L).build();
    assertThat(resourceState.getResourceId()).isEqualTo(1L);
    assertThat(resourceState.getTimestamp()).isEqualTo(-1L);
    assertThat(resourceState.getEventType()).isEqualTo("");
    assertThat(resourceState.getPlatformId()).isEqualTo(-1L);
    assertThat(resourceState.getAvailableResources()).isEqualTo(-1.0);
    assertThat(resourceState.getAvailableMemory()).isEqualTo(-1.0);
    assertThat(resourceState.getAvailableDiskSpace()).isEqualTo(-1.0);
    assertThat(resourceState.getAvailableDiskIoBandwidth()).isEqualTo(-1.0);
    assertThat(resourceState.getAvailableNetworkBandwidth()).isEqualTo(-1.0);
    assertThat(resourceState.getAverageUtilization1Minute()).isEqualTo(-1.0);
    assertThat(resourceState.getAverageUtilization5Minute()).isEqualTo(-1.0);
    assertThat(resourceState.getAverageUtilization15Minute()).isEqualTo(-1.0);
  }
}
