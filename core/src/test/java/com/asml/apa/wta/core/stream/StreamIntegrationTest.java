package com.asml.apa.wta.core.stream;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.List;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Fixture for stream integration testing on serialization.
 */
class StreamIntegrationTest {

  private static final int defaultSerTrigger = 10;

  private static final Path serializationDirectory = Path.of("tmp/wta/streams/serialization/");

  Stream<Integer> createSerializingStreamOfNaturalNumbers(int positiveSize, int serializationTrigger) {
    Stream<Integer> stream = new Stream<>(0, serializationTrigger);
    for (int i = 1; i <= positiveSize; i++) {
      stream.addToStream(i);
    }
    return stream;
  }

  @BeforeAll
  static void setUpTmpDirectory() throws IOException {
    new File("tmp/wta/streams/serialization/").mkdirs();
    if (!Files.exists(Path.of("tmp/wta/streams/serialization/"))) {
      throw new IOException();
    }
  }

  @Test
  void streamSerializationWithMap() {
    Stream<Integer> stream = createSerializingStreamOfNaturalNumbers(10, defaultSerTrigger);
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
  void streamSerializationWithFilter() {
    Stream<Integer> stream = createSerializingStreamOfNaturalNumbers(10, defaultSerTrigger);
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
  void streamSerializationWithFoldLeft() {
    Stream<Integer> stream = createSerializingStreamOfNaturalNumbers(11, defaultSerTrigger);
    for (int i = 1; i <= 9; i++) {
      stream.addToStream(i);
    }
    stream.addToStream(10);
    stream.addToStream(5);
    assertThat(stream.foldLeft(0, Integer::sum)).isEqualTo(126);
    assertThat(stream.isEmpty()).isTrue();
  }

  @Test
  void streamSerializationWithHead() {
    Stream<Integer> stream = createSerializingStreamOfNaturalNumbers(10, defaultSerTrigger);
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

  @Test
  void streamWithHugeIntegersGetsRetrievedCorrectlyIntoAList() {
    Stream<Integer> stream = createSerializingStreamOfNaturalNumbers(0, 100);
    for (int i = 1; i <= 20000; i++) {
      stream.addToStream(i);
    }

    List<Integer> sutList = stream.toList();

    // assert that sutList has numbers from 0 to 20000 in order
    assertThat(sutList).hasSize(20001);
    assertThat(sutList).isSortedAccordingTo(Comparator.naturalOrder());
    for (int i = 0; i <= 20000; i++) {
      assertThat(sutList.get(i)).isEqualTo(i);
    }
  }

  @Test
  void serializationFilesActuallyGetGeneratedAndDeleted() throws IOException {
    long startingFileCount = Files.list(serializationDirectory).count();
    Stream<Integer> stream = createSerializingStreamOfNaturalNumbers(10, 10);
    for (int i = 1; i <= 10; i++) {
      stream.addToStream(i);
    }
    for (int i = 1; i <= 10; i++) {
      stream.addToStream(i);
    }
    for (int i = 1; i <= 10; i++) {
      stream.addToStream(i);
    }
    long fileCount = Files.list(serializationDirectory).count();
    assertThat(fileCount).isEqualTo(startingFileCount + 3);
    Stream.deleteAllSerializedFiles();
    assertThat(Files.exists(serializationDirectory)).isFalse();
  }

  @Test
  void streamSerializationWithClone() {
    Stream<Integer> originalStream = createSerializingStreamOfNaturalNumbers(10, defaultSerTrigger);
    for (int i = 1; i <= 9; i++) {
      originalStream.addToStream(i);
    }
    originalStream.addToStream(10);
    originalStream.addToStream(5);
    Stream<Integer> clone = originalStream.copy();
    for (Stream<Integer> stream : List.of(originalStream, clone)) {
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
  }

  @Test
  void streamSerializationWithPartiallyConsumedClone() {
    Stream<Integer> originalStream = createSerializingStreamOfNaturalNumbers(10, defaultSerTrigger);
    for (int i = 1; i <= 9; i++) {
      originalStream.addToStream(i);
    }
    originalStream.addToStream(10);
    originalStream.addToStream(5);
    int head = originalStream.head();
    assertThat(head).isEqualTo(0);
    for (int i = 1; i <= 10; i++) {
      originalStream.addToStream(i);
    }
    Stream<Integer> clone = originalStream.copy();
    for (Stream<Integer> stream : List.of(originalStream, clone)) {
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
      assertThat(stream.isEmpty()).isTrue();
    }
  }
}
