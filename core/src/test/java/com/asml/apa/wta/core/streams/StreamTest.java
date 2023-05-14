package com.asml.apa.wta.core.streams;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.asml.apa.wta.core.exceptions.FailedToDeserializeStreamException;
import com.asml.apa.wta.core.exceptions.FailedToSerializeStreamException;
import com.asml.apa.wta.core.exceptions.StreamSerializationException;
import java.util.NoSuchElementException;
import org.junit.jupiter.api.Test;

/**
 * Fixture for {@link com.asml.apa.wta.core.streams.Stream}.
 */
class StreamTest {

  Stream<Integer> createStreamOfNaturalNumbers(int size) throws FailedToSerializeStreamException {
    Stream<Integer> stream = new Stream<>();
    for (int i = 1; i <= size; i++) {
      stream.addToStream(i);
    }
    return stream;
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
  void mapStream() throws StreamSerializationException {
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
  void filterStream() throws StreamSerializationException {
    Stream<Integer> stream = createStreamOfNaturalNumbers(11);
    Stream<Integer> filteredStream = stream.filter((i) -> i > 9);
    assertThat(filteredStream.head()).isEqualTo(10);
    assertThat(filteredStream.head()).isEqualTo(11);
    assertThat(filteredStream.isEmpty()).isTrue();
  }

  @Test
  void foldStream() throws FailedToDeserializeStreamException, FailedToSerializeStreamException {
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
  void peekAtStream() {
    Stream<Integer> stream = new Stream<>(2);
    assertThat(stream.peek()).isEqualTo(2);
    assertThat(stream.isEmpty()).isFalse();
  }

  @Test
  void peekAtEmptyStream() {
    Stream<Integer> stream = new Stream<>();
    assertThatThrownBy(stream::peek).isInstanceOf(NoSuchElementException.class);
  }

  @Test
  void mapUsingNullOperation() throws FailedToSerializeStreamException {
    Stream<Integer> stream = createStreamOfNaturalNumbers(1309);
    assertThatThrownBy(() -> stream.map(null)).isInstanceOf(NullPointerException.class);
  }

  @Test
  void filterUsingNullOperation() throws FailedToSerializeStreamException {
    Stream<Integer> stream = createStreamOfNaturalNumbers(102);
    assertThatThrownBy(() -> stream.filter(null)).isInstanceOf(NullPointerException.class);
  }

  @Test
  void foldUsingNullOperation() throws FailedToSerializeStreamException {
    Stream<Integer> stream = createStreamOfNaturalNumbers(457);
    assertThatThrownBy(() -> stream.foldLeft(0, null)).isInstanceOf(NullPointerException.class);
  }

  @Test
  void simpleStreamWorkflow() throws FailedToDeserializeStreamException, FailedToSerializeStreamException {
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
}
