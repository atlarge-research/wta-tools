package com.asml.apa.wta.core.streams;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.asml.apa.wta.core.exceptions.FailedToDeserializeStreamException;
import com.asml.apa.wta.core.exceptions.FailedToSerializeStreamException;
import com.asml.apa.wta.core.exceptions.StreamSerializationException;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.NoSuchElementException;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Fixture for {@link com.asml.apa.wta.core.streams.Stream}.
 */
class StreamTest {

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
  void setsUpEmptyStream() {
    Stream<Integer> stream = new Stream<>();
    assertThat(stream.isEmpty()).isTrue();
  }

  @Test
  void setsUpStreamWithOneElement() throws FailedToDeserializeStreamException {
    Stream<Integer> stream = new Stream<>(2);
    assertThat(stream.isEmpty()).isFalse();
    assertThat(stream.head()).isEqualTo(2);
  }

  @Test
  void mapStream() throws FailedToDeserializeStreamException {
    Stream<Integer> stream = createStreamOfNaturalNumbers(10);
    Stream<Integer> mappedStream = stream.map((i) -> {
      if (i < 3) {
        return 0;
      } else {
        return 3;
      }
    });
    assertThat(mappedStream.head()).isEqualTo(0);
    assertThat(mappedStream.head()).isEqualTo(0);
    assertThat(mappedStream.head()).isEqualTo(3);
    assertThat(mappedStream.head()).isEqualTo(3);
  }

  @Test
  void filterStream() throws FailedToDeserializeStreamException {
    Stream<Integer> stream = createStreamOfNaturalNumbers(11);
    Stream<Integer> filteredStream = stream.filter((i) -> i > 9);
    assertThat(filteredStream.head()).isEqualTo(10);
    assertThat(filteredStream.head()).isEqualTo(11);
    assertThat(filteredStream.isEmpty()).isTrue();
  }

  @Test
  void foldStream() throws FailedToDeserializeStreamException {
    Stream<Integer> stream = createStreamOfNaturalNumbers(10);
    int sum = stream.foldLeft(0, Integer::sum);
    assertThat(sum).isEqualTo(55);
  }

  @Test
  void headOfEmptyStream() {
    Stream<Integer> stream = new Stream<>();
    assertThatThrownBy(stream::head).isInstanceOf(NoSuchElementException.class);
  }

  @Test
  void mapUsingNullOperation() {
    Stream<Integer> stream = createStreamOfNaturalNumbers(1309);
    assertThatThrownBy(() -> stream.map(null)).isInstanceOf(NullPointerException.class);
  }

  @Test
  void filterUsingNullOperation() {
    Stream<Integer> stream = createStreamOfNaturalNumbers(102);
    assertThatThrownBy(() -> stream.filter(null)).isInstanceOf(NullPointerException.class);
  }

  @Test
  void foldUsingNullOperation() {
    Stream<Integer> stream = createStreamOfNaturalNumbers(457);
    assertThatThrownBy(() -> stream.foldLeft(0, null)).isInstanceOf(NullPointerException.class);
  }

  @Test
  void simpleStreamWorkflow() throws FailedToDeserializeStreamException {
    Stream<Integer> stream = createStreamOfNaturalNumbers(10);
    int one = stream.head();
    stream.addToStream(1);
    int two = stream.head();
    stream.addToStream(2);
    int sum = stream.foldLeft(0, Integer::sum);
    assertThat(one).isEqualTo(1);
    assertThat(two).isEqualTo(2);
    assertThat(sum).isEqualTo(55);
  }

  @Test
  void streamSerializationWithManualDeserialization() throws StreamSerializationException {
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
  void streamSerializationWithMapAndAutomaticDeserialization()
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
  void streamSerializationWithFilterAndAutomaticDeserialization()
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
  void streamSerializationWithFoldLeftAndAutomaticDeserialization() throws StreamSerializationException {
    Stream<Integer> stream = createStreamOfNaturalNumbers(10);
    stream.serializeInternals();
    for (int i = 1; i <= 9; i++) {
      stream.addToStream(i);
    }
    stream.serializeInternals();
    stream.addToStream(10);
    stream.addToStream(5);
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
  void streamSerializationWithHeadAndAutomaticDeserialization() throws StreamSerializationException {
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
