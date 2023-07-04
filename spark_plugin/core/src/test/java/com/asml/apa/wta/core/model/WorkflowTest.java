package com.asml.apa.wta.core.model;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class WorkflowTest {

  @Test
  void defaultBuilderValues() {
    Workflow workflow = Workflow.builder().id(1L).domain(Domain.BIOMEDICAL).build();
    assertThat(workflow.getId()).isEqualTo(1L);
    assertThat(workflow.getDomain()).isEqualTo(Domain.BIOMEDICAL);
    assertThat(workflow.getNfrs()).isEqualTo("");
    assertThat(workflow.getScheduler()).isEqualTo("");
    assertThat(workflow.getApplicationName()).isEqualTo("");
    assertThat(workflow.getApplicationField()).isEqualTo("ETL");
    assertThat(workflow.getTotalResources()).isEqualTo(-1.0);
    assertThat(workflow.getTotalMemoryUsage()).isEqualTo(-1.0);
    assertThat(workflow.getTsSubmit()).isEqualTo(-1L);
    assertThat(workflow.getTaskIds()).isEqualTo(new Long[0]);
    assertThat(workflow.getTaskCount()).isEqualTo(-1L);
    assertThat(workflow.getCriticalPathLength()).isEqualTo(-1L);
    assertThat(workflow.getCriticalPathTaskCount()).isEqualTo(-1L);
    assertThat(workflow.getMaxConcurrentTasks()).isEqualTo(-1);
  }
}
