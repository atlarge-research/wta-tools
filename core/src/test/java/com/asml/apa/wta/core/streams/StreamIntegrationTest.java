package com.asml.apa.wta.core.streams;

import static org.assertj.core.api.Assertions.assertThat;

import com.asml.apa.wta.core.exceptions.FailedToSerializeStreamException;
import com.asml.apa.wta.core.exceptions.StreamSerializationException;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import lombok.NonNull;
import net.jqwik.api.Arbitraries;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.Provide;
import net.jqwik.api.constraints.UniqueElements;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Fixture for stream integration testing on serialization.
 */
public class StreamIntegrationTest {

  Stream<Integer> createSerializingStreamOfNaturalNumbers(int size) throws FailedToSerializeStreamException {
    Stream<Integer> stream = new Stream<>(0, 10);
    for (int i = 1; i <= size; i++) {
      stream.addToStream(i);
    }
    return stream;
  }

  @Provide
  Arbitrary<List<Integer>> largeListOfIntegers() {
    return Arbitraries.integers().between(-65536, 65536).collect(list -> list.size() >= 1800);
  }

  @Provide
  Arbitrary<List<Double>> largeListOfDoubles() {
    return Arbitraries.doubles().collect(list -> list.size() >= 1800);
  }

  @Provide
  Arbitrary<List<String>> largeListOfLargeStrings() {
    return Arbitraries.strings().ofMinLength(10).ofMaxLength(20).collect(list -> list.size() >= 1800);
  }

  @Provide
  Arbitrary<List<String>> largeListOfStrings() {
    return Arbitraries.strings().ofMinLength(0).ofMaxLength(10).collect(list -> list.size() >= 1800);
  }

  @BeforeAll
  static void setUpTmpDirectory() throws IOException {
    new File("tmp").mkdirs();
    if (!Files.exists(Path.of("tmp"))) {
      throw new IOException();
    }
  }

  @Test
  void streamSerializationWithMap() throws StreamSerializationException {
    Stream<Integer> stream = createSerializingStreamOfNaturalNumbers(10);
    for (int i = 1; i <= 10; i++) {
      stream.addToStream(i);
    }
    for (int i = 1; i <= 10; i++) {
      stream.addToStream(i);
    }
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
  void streamSerializationWithFilter() throws StreamSerializationException {
    Stream<Integer> stream = createSerializingStreamOfNaturalNumbers(10);
    for (int i = 1; i <= 10; i++) {
      stream.addToStream(i);
    }
    stream.addToStream(1);
    for (int i = 10; i > 0; i--) {
      stream.addToStream(i);
    }
    for (int i = 1; i <= 10; i++) {
      stream.addToStream(i);
    }
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
  void streamSerializationWithFoldLeft() throws StreamSerializationException {
    Stream<Integer> stream = createSerializingStreamOfNaturalNumbers(11);
    for (int i = 1; i <= 9; i++) {
      stream.addToStream(i);
    }
    stream.addToStream(10);
    stream.addToStream(5);
    //    assertThat(stream.foldLeft(0, Integer::sum)).isEqualTo(126);
    assertThat(stream.head()).isEqualTo(0);
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
  void streamSerializationWithHead() throws StreamSerializationException {
    Stream<Integer> stream = createSerializingStreamOfNaturalNumbers(10);
    for (int i = 1; i <= 9; i++) {
      stream.addToStream(i);
    }
    stream.addToStream(10);
    stream.addToStream(5);
    assertThat(stream.head()).isEqualTo(0);
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

  @Property
  void propertyBasedFoldLeftOnStreams(@ForAll("largeListOfIntegers") @NonNull List<Integer> integers)
      throws StreamSerializationException {
    Stream<Integer> stream = new Stream<>();
    int expected = 0;
    for (int i : integers) {
      stream.addToStream(i);
      expected += i;
    }
    assertThat(stream.foldLeft(0, Integer::sum)).isEqualTo(expected);
  }

  @Property
  void propertyBasedMapOnStreams(@ForAll("largeListOfDoubles") @NonNull List<Double> doubles)
      throws StreamSerializationException {
    Stream<Double> stream = new Stream<>();
    for (double d : doubles) {
      stream.addToStream(d);
    }
    double expected = Math.pow(doubles.get(0), 2.0) + 7.22;
    assertThat(stream.map(i -> Math.pow(i, 2.0) + 7.22).head()).isEqualTo(expected);
  }

  @Property
  void propertyBasedFilterOnStreams(@ForAll("largeListOfStrings") @NonNull List<String> strings)
      throws StreamSerializationException {
    Stream<String> stream = new Stream<>();
    int expectedLength = 0;
    for (String s : strings) {
      stream.addToStream(s);
      if (s.length() > 3) {
        expectedLength++;
      }
    }
    stream = stream.filter(s -> s.length() > 3);
    int actualLength = 0;
    while (!stream.isEmpty()) {
      stream.head();
      actualLength++;
    }
    assertThat(actualLength).isEqualTo(expectedLength);
  }

  @Property
  void propertyBasedPeekObjectWillBeTheSameAsHead(
      @ForAll("largeListOfLargeStrings") @UniqueElements @NonNull List<String> strings)
      throws StreamSerializationException {
    Stream<String> stream = new Stream<>();
    for (String s : strings) {
      stream.addToStream(s);
    }
    int amountOfElements = 0;
    while (!stream.isEmpty()) {
      amountOfElements++;
      assertThat(stream.peek()).isEqualTo(stream.head());
    }
    assertThat(amountOfElements).isEqualTo(strings.size());
  }

  @Property
  void propertyBasedHeadObjectWillNotBeTheSameAsPeekAfterwards(
      @ForAll("largeListOfLargeStrings") @UniqueElements @NonNull List<String> strings)
      throws StreamSerializationException {
    Stream<String> stream = new Stream<>();
    for (String s : strings) {
      stream.addToStream(s);
    }
    int amountOfElements = 0;
    while (true) {
      String head = stream.head();
      amountOfElements++;
      if (!stream.isEmpty()) {
        assertThat(head).isNotEqualTo(stream.peek());
      } else {
        break;
      }
    }
    assertThat(amountOfElements).isEqualTo(strings.size());
  }
}
