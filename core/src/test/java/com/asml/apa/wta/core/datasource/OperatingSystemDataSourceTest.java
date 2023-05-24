package com.asml.apa.wta.core.datasource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.spy;

import org.junit.jupiter.api.Test;

class OperatingSystemDataSourceTest {

  OperatingSystemDataSource sut = spy(OperatingSystemDataSource.class);

  @Test
  void getCommittedVirtualMemorySize() {
    assertThat(sut.isValid()).isTrue();
    assertThat(sut.getCommittedVirtualMemorySize()).isGreaterThanOrEqualTo(-1);
  }

  @Test
  void getFreePhysicalMemorySize() {
    assertThat(sut.isValid()).isTrue();
    assertThat(sut.getFreePhysicalMemorySize()).isGreaterThanOrEqualTo(0);
  }

  @Test
  void getProcessCpuLoad() {
    assertThat(sut.isValid()).isTrue();
    double load = sut.getProcessCpuLoad();
    assertThat(load).isGreaterThanOrEqualTo(0.0);
    assertThat(load).isLessThanOrEqualTo(1.0);
  }

  @Test
  void getProcessCpuTime() {
    assertThat(sut.isValid()).isTrue();
    assertThat(sut.getProcessCpuTime()).isGreaterThanOrEqualTo(-1);
  }

  @Test
  void getTotalPhysicalMemorySize() {
    assertThat(sut.isValid()).isTrue();
    assertThat(sut.getTotalPhysicalMemorySize()).isGreaterThanOrEqualTo(0);
  }

  @Test
  void getSystemLoadAverage() {
    assertThat(sut.isValid()).isTrue();
    assertThat(sut.getSystemLoadAverage()).isLessThanOrEqualTo(1.0);
  }

  @Test
  void getAvailableProcessors() {
    assertThat(sut.isValid()).isTrue();
    assertThat(sut.getAvailableProcessors()).isGreaterThanOrEqualTo(1);
  }
}
