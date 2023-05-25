package com.asml.apa.wta.core.streams;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import lombok.NonNull;
import net.jqwik.api.Arbitraries;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.Provide;
import net.jqwik.api.constraints.IntRange;
import net.jqwik.api.constraints.Size;
import net.jqwik.api.constraints.UniqueElements;

/**
 * Fixture for integration testing with {@link com.asml.apa.wta.core.streams.KeyedStream}.
 */
class KeyedStreamIntegrationTest {

  @Provide
  @SuppressWarnings("unused")
  Arbitrary<List<String>> listOfStrings() {
    return Arbitraries.strings().ofMinLength(10).ofMaxLength(20).collect(list -> list.size() >= 1800);
  }

  @Property
  void simpleOnKeyRetrieval(
      @ForAll("listOfStrings") @NonNull List<String> strings,
      @ForAll @IntRange(min = -10, max = 10) int key,
      @ForAll @Size(100) @UniqueElements List<Integer> list) {
    KeyedStream<Integer, String> keyedStream = new KeyedStream<>();
    for (String s : strings) {
      keyedStream.addToStream(key, s);
    }
    assertThat(keyedStream.onKey(key).isEmpty()).isFalse();
    for (int i : list) {
      if (i != key) {
        assertThat(keyedStream.onKey(i).isEmpty()).isTrue();
      }
    }
  }
}
