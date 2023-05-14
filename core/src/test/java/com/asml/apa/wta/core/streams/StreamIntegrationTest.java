package com.asml.apa.wta.core.streams;

import static org.assertj.core.api.Assertions.assertThat;

import com.asml.apa.wta.core.exceptions.FailedToDeserializeStreamException;
import com.asml.apa.wta.core.exceptions.FailedToSerializeStreamException;
import com.asml.apa.wta.core.exceptions.StreamSerializationException;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Fixture for stream integration testing on serialization
 */
public class StreamIntegrationTest {

  Stream<Integer> createStreamOfNaturalNumbers(int size) {
    Stream<Integer> stream = new Stream<>();
    for (int i = 1; i <= size; i++) {
      stream.addToStream(i);
    }
    return stream;
  }

  @BeforeAll
  static void setUpTmpDirectory() throws IOException {
    new File("tmp").mkdirs();
    if (!Files.exists(Path.of("tmp"))) {
      throw new IOException();
    }
  }

  @Test
  void manualStreamSerializationWithManualDeserialization() throws StreamSerializationException {
    Stream<Integer> stream = createStreamOfNaturalNumbers(10);
    stream.serializeInternals();
    for (int i = 1; i <= 9; i++) {
      stream.addToStream(i);
    }
    stream.serializeInternals();
    stream.addToStream(10);
    stream.addToStream(5);
    stream.deserializeAll();
    assertThat(stream.foldLeft(0, Integer::sum)).isEqualTo(115);
    assertThat(stream.head()).isEqualTo(1);
    assertThat(stream.head()).isEqualTo(2);
    assertThat(stream.head()).isEqualTo(3);
    assertThat(stream.head()).isEqualTo(4);
    assertThat(stream.head()).isEqualTo(5);
    assertThat(stream.head()).isEqualTo(6);
    assertThat(stream.head()).isEqualTo(7);
    assertThat(stream.head()).isEqualTo(8);
    assertThat(stream.head()).isEqualTo(9);
    assertThat(stream.head()).isEqualTo(10);
    assertThat(stream.head()).isEqualTo(1);
    assertThat(stream.head()).isEqualTo(2);
    assertThat(stream.head()).isEqualTo(3);
    assertThat(stream.head()).isEqualTo(4);
    assertThat(stream.head()).isEqualTo(5);
    assertThat(stream.head()).isEqualTo(6);
    assertThat(stream.head()).isEqualTo(7);
    assertThat(stream.head()).isEqualTo(8);
    assertThat(stream.head()).isEqualTo(9);
    assertThat(stream.head()).isEqualTo(10);
    assertThat(stream.head()).isEqualTo(5);
    assertThat(stream.isEmpty()).isTrue();
  }

  @Test
  void manualStreamSerializationWithMapAndAutomaticDeserialization()
      throws FailedToSerializeStreamException, FailedToDeserializeStreamException {
    Stream<Integer> stream = createStreamOfNaturalNumbers(10);
    stream.serializeInternals();
    for (int i = 1; i <= 10; i++) {
      stream.addToStream(i);
    }
    stream.serializeInternals();
    for (int i = 1; i <= 10; i++) {
      stream.addToStream(i);
    }
    stream.serializeInternals();
    for (int i = 1; i <= 10; i++) {
      stream.addToStream(i);
    }
    stream = stream.map((i) -> {
      if (i > 5) {
        return 5;
      } else {
        return 0;
      }
    });
    assertThat(stream.head()).isEqualTo(0);
    assertThat(stream.head()).isEqualTo(0);
    assertThat(stream.head()).isEqualTo(0);
    assertThat(stream.head()).isEqualTo(0);
    assertThat(stream.head()).isEqualTo(0);
    assertThat(stream.head()).isEqualTo(5);
    assertThat(stream.head()).isEqualTo(5);
    assertThat(stream.head()).isEqualTo(5);
    assertThat(stream.head()).isEqualTo(5);
    assertThat(stream.head()).isEqualTo(5);
    assertThat(stream.head()).isEqualTo(0);
    assertThat(stream.head()).isEqualTo(0);
    assertThat(stream.head()).isEqualTo(0);
    assertThat(stream.head()).isEqualTo(0);
    assertThat(stream.head()).isEqualTo(0);
    assertThat(stream.head()).isEqualTo(5);
    assertThat(stream.head()).isEqualTo(5);
    assertThat(stream.head()).isEqualTo(5);
    assertThat(stream.head()).isEqualTo(5);
    assertThat(stream.head()).isEqualTo(5);
    assertThat(stream.head()).isEqualTo(0);
    assertThat(stream.head()).isEqualTo(0);
    assertThat(stream.head()).isEqualTo(0);
    assertThat(stream.head()).isEqualTo(0);
    assertThat(stream.head()).isEqualTo(0);
    assertThat(stream.head()).isEqualTo(5);
    assertThat(stream.head()).isEqualTo(5);
    assertThat(stream.head()).isEqualTo(5);
    assertThat(stream.head()).isEqualTo(5);
    assertThat(stream.head()).isEqualTo(5);
    assertThat(stream.head()).isEqualTo(0);
    assertThat(stream.head()).isEqualTo(0);
    assertThat(stream.head()).isEqualTo(0);
    assertThat(stream.head()).isEqualTo(0);
    assertThat(stream.head()).isEqualTo(0);
    assertThat(stream.head()).isEqualTo(5);
    assertThat(stream.head()).isEqualTo(5);
    assertThat(stream.head()).isEqualTo(5);
    assertThat(stream.head()).isEqualTo(5);
    assertThat(stream.head()).isEqualTo(5);
    assertThat(stream.isEmpty()).isTrue();
  }

  @Test
  void manualStreamSerializationWithFilterAndAutomaticDeserialization()
      throws FailedToSerializeStreamException, FailedToDeserializeStreamException {
    Stream<Integer> stream = createStreamOfNaturalNumbers(10);
    stream.serializeInternals();
    for (int i = 1; i <= 10; i++) {
      stream.addToStream(i);
    }
    stream.addToStream(1);
    stream.serializeInternals();
    for (int i = 10; i > 0; i--) {
      stream.addToStream(i);
    }
    for (int i = 1; i <= 10; i++) {
      stream.addToStream(i);
    }
    stream.serializeInternals();
    for (int i = 1; i <= 10; i++) {
      stream.addToStream(i);
    }
    stream = stream.filter((i) -> i > 5);
    assertThat(stream.head()).isEqualTo(6);
    assertThat(stream.head()).isEqualTo(7);
    assertThat(stream.head()).isEqualTo(8);
    assertThat(stream.head()).isEqualTo(9);
    assertThat(stream.head()).isEqualTo(10);
    assertThat(stream.head()).isEqualTo(6);
    assertThat(stream.head()).isEqualTo(7);
    assertThat(stream.head()).isEqualTo(8);
    assertThat(stream.head()).isEqualTo(9);
    assertThat(stream.head()).isEqualTo(10);
    assertThat(stream.head()).isEqualTo(10);
    assertThat(stream.head()).isEqualTo(9);
    assertThat(stream.head()).isEqualTo(8);
    assertThat(stream.head()).isEqualTo(7);
    assertThat(stream.head()).isEqualTo(6);
    assertThat(stream.head()).isEqualTo(6);
    assertThat(stream.head()).isEqualTo(7);
    assertThat(stream.head()).isEqualTo(8);
    assertThat(stream.head()).isEqualTo(9);
    assertThat(stream.head()).isEqualTo(10);
    assertThat(stream.head()).isEqualTo(6);
    assertThat(stream.head()).isEqualTo(7);
    assertThat(stream.head()).isEqualTo(8);
    assertThat(stream.head()).isEqualTo(9);
    assertThat(stream.head()).isEqualTo(10);
    assertThat(stream.isEmpty()).isTrue();
  }

  @Test
  void manualStreamSerializationWithFoldLeftAndAutomaticDeserialization() throws StreamSerializationException {
    Stream<Integer> stream = createStreamOfNaturalNumbers(11);
    stream.serializeInternals();
    for (int i = 1; i <= 9; i++) {
      stream.addToStream(i);
    }
    stream.serializeInternals();
    stream.addToStream(10);
    stream.addToStream(5);
    assertThat(stream.foldLeft(0, Integer::sum)).isEqualTo(126);
    assertThat(stream.head()).isEqualTo(1);
    assertThat(stream.head()).isEqualTo(2);
    assertThat(stream.head()).isEqualTo(3);
    assertThat(stream.head()).isEqualTo(4);
    assertThat(stream.head()).isEqualTo(5);
    assertThat(stream.head()).isEqualTo(6);
    assertThat(stream.head()).isEqualTo(7);
    assertThat(stream.head()).isEqualTo(8);
    assertThat(stream.head()).isEqualTo(9);
    assertThat(stream.head()).isEqualTo(10);
    assertThat(stream.head()).isEqualTo(11);
    assertThat(stream.head()).isEqualTo(1);
    assertThat(stream.head()).isEqualTo(2);
    assertThat(stream.head()).isEqualTo(3);
    assertThat(stream.head()).isEqualTo(4);
    assertThat(stream.head()).isEqualTo(5);
    assertThat(stream.head()).isEqualTo(6);
    assertThat(stream.head()).isEqualTo(7);
    assertThat(stream.head()).isEqualTo(8);
    assertThat(stream.head()).isEqualTo(9);
    assertThat(stream.head()).isEqualTo(10);
    assertThat(stream.head()).isEqualTo(5);
    assertThat(stream.isEmpty()).isTrue();
  }

  @Test
  void manualStreamSerializationWithHeadAndAutomaticDeserialization() throws StreamSerializationException {
    Stream<Integer> stream = createStreamOfNaturalNumbers(10);
    stream.serializeInternals();
    for (int i = 1; i <= 9; i++) {
      stream.addToStream(i);
    }
    stream.serializeInternals();
    stream.addToStream(10);
    stream.addToStream(5);
    assertThat(stream.head()).isEqualTo(1);
    assertThat(stream.head()).isEqualTo(2);
    assertThat(stream.head()).isEqualTo(3);
    assertThat(stream.head()).isEqualTo(4);
    assertThat(stream.head()).isEqualTo(5);
    assertThat(stream.head()).isEqualTo(6);
    assertThat(stream.head()).isEqualTo(7);
    assertThat(stream.head()).isEqualTo(8);
    assertThat(stream.head()).isEqualTo(9);
    assertThat(stream.head()).isEqualTo(10);
    assertThat(stream.head()).isEqualTo(1);
    assertThat(stream.head()).isEqualTo(2);
    assertThat(stream.head()).isEqualTo(3);
    assertThat(stream.head()).isEqualTo(4);
    assertThat(stream.head()).isEqualTo(5);
    assertThat(stream.head()).isEqualTo(6);
    assertThat(stream.head()).isEqualTo(7);
    assertThat(stream.head()).isEqualTo(8);
    assertThat(stream.head()).isEqualTo(9);
    assertThat(stream.head()).isEqualTo(10);
    assertThat(stream.head()).isEqualTo(5);
    assertThat(stream.isEmpty()).isTrue();
  }
}
