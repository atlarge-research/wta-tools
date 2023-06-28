package com.asml.apa.wta.core.util;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

import com.asml.apa.wta.core.stream.Stream;
import java.util.List;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

class KthSmallestTest {

  static KthSmallest kthSmallest;

  @BeforeAll
  static void setUp() {
    kthSmallest = new KthSmallest();
  }

  @Test
  void getMedianOfStream() {
    List<Double> list = List.of(1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0);
    double median = kthSmallest.find(new Stream<>(list), 3);
    assertThat(median).isEqualTo(4.0);
  }

  @Test
  void getSmallestOfStream() {
    List<Double> list = List.of(1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0);
    double median = kthSmallest.find(new Stream<>(list), 0);
    assertThat(median).isEqualTo(1.0);
  }

  @Test
  void getLargestOfStream() {
    List<Double> list = List.of(1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0);
    double median = kthSmallest.find(new Stream<>(list), 6);
    assertThat(median).isEqualTo(7.0);
  }

  @Test
  void getKthOfUniform() {
    List<Double> list = List.of(-1.0, -1.0, -1.0, -1.0, -1.0, -1.0, -1.0, -1.0);
    double median = kthSmallest.find(new Stream<>(list), 5);
    assertThat(median).isEqualTo(-1.0);
  }
}
