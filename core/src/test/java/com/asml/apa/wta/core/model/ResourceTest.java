package com.asml.apa.wta.core.model;

import org.junit.jupiter.api.Test;
import java.util.HashMap;
import static org.assertj.core.api.Assertions.assertThat;

class ResourceTest {

  @Test
  void defaultBuilderValues() {
    Resource resource = Resource.builder().id(1L).build();
    assertThat(resource.getId()).isEqualTo(1L);
    assertThat(resource.getType()).isEqualTo("cluster node");
    assertThat(resource.getNumResources()).isEqualTo(1.0);
    assertThat(resource.getProcModel()).isEqualTo("unknown");
    assertThat(resource.getMemory()).isEqualTo(-1L);
    assertThat(resource.getDiskSpace()).isEqualTo(-1L);
    assertThat(resource.getNetwork()).isEqualTo(-1L);
    assertThat(resource.getOs()).isEqualTo("unknown");
    assertThat(resource.getDetails()).isEqualTo("");
    assertThat(resource.getEvents()).isEqualTo(new HashMap<>());
  }
}
