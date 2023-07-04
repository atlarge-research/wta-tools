package com.asml.apa.wta.core.model;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class TaskTest {

  @Test
  void defaultBuilderValues() {
    Task task = Task.builder().id(1L).build();
    assertThat(task.getId()).isEqualTo(1L);
    assertThat(task.getType()).isEqualTo("");
    assertThat(task.getTsSubmit()).isEqualTo(-1L);
    assertThat(task.getWorkflowId()).isEqualTo(-1L);
    assertThat(task.getSubmissionSite()).isEqualTo(-1);
    assertThat(task.getRuntime()).isEqualTo(-1L);
    assertThat(task.getResourceType()).isEqualTo("N/A");
    assertThat(task.getResourceAmountRequested()).isEqualTo(-1.0);
    assertThat(task.getParents()).isEqualTo(new long[0]);
    assertThat(task.getChildren()).isEqualTo(new long[0]);
    assertThat(task.getUserId()).isEqualTo(-1);
    assertThat(task.getGroupId()).isEqualTo(-1);
    assertThat(task.getNfrs()).isEqualTo("");
    assertThat(task.getWaitTime()).isEqualTo(-1L);
    assertThat(task.getParams()).isEqualTo("");
    assertThat(task.getMemoryRequested()).isEqualTo(-1.0);
    assertThat(task.getDiskIoTime()).isEqualTo(-1L);
    assertThat(task.getDiskSpaceRequested()).isEqualTo(-1.0);
    assertThat(task.getEnergyConsumption()).isEqualTo(-1L);
    assertThat(task.getNetworkIoTime()).isEqualTo(-1L);
    assertThat(task.getResourceUsed()).isEqualTo(-1L);
  }
}
