package com.asml.apa.wta.core.streams;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

/**
 * Fixture for {@link com.asml.apa.wta.core.streams.KeyedStream}.
 */
class KeyedStreamTest {

  @Test
  void integerKeyedInteractions() {
    KeyedStream<Integer, String> keyedStream = new KeyedStream<>();
    keyedStream.addToStream(2, "Hello");
    keyedStream.addToStream(-3, "World!");
    String stringOne = keyedStream.onKey(2).head();
    String stringTwo = keyedStream.onKey(-3).head();
    assertThat(stringOne).isEqualTo("Hello");
    assertThat(stringTwo).isEqualTo("World!");
  }

  @Test
  void onKeyInEmptyStream() {
    KeyedStream<Integer, String> keyedStream = new KeyedStream<>();
    Stream<String> emptyStream = keyedStream.onKey(2);
    assertThat(emptyStream.isEmpty()).isTrue();
  }

  @Test
  void onKeyThatDoesNotExist() {
    KeyedStream<Integer, String> keyedStream = new KeyedStream<>();
    keyedStream.addToStream(-2, "Hello");
    keyedStream.addToStream(3, "World!");
    Stream<String> emptyStream = keyedStream.onKey(17);
    assertThat(emptyStream.isEmpty()).isTrue();
  }
}
