package com.asml.apa.wta.core.streams;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * General test fixture for the {@link com.asml.apa.wta.core.streams.KeyedStream} class' methods.
 */
class KeyedStreamTest {

  private KeyedStream<Integer, DummyStreamRecord> stream;

  @BeforeEach
  void setUp() {
    stream = new KeyedStream<>();
  }

  @Test
  void addsNullRecordToStream() {
    assertThrows(NullPointerException.class, () -> stream.addToStream(3, null));
  }

  @Test
  void addsNullKeyToStream() {
    assertThrows(NullPointerException.class, () -> stream.addToStream(null, new DummyStreamRecord()));
  }

  @Test
  void addsAllNullsToStream() {
    assertThrows(NullPointerException.class, () -> stream.addToStream(null, null));
  }

  @Test
  void addsValidEntryToStream() {
    assertDoesNotThrow(() -> stream.addToStream(3, new DummyStreamRecord()));
  }

  @Test
  void addsEntriesToKeyedStream() {
    stream.addToStream(1, new DummyStreamRecord());
    stream.addToStream(2, new DummyStreamRecord());
    stream.addToStream(3, new DummyStreamRecord());
    stream.addToStream(1, new DummyStreamRecord());
    assertFalse(stream.onKey(1).isEmpty());
    assertTrue(stream.onKey(2).isEmpty());
  }

  @Test
  void onKeyWithNullOnEmptyStream() {
    assertThrows(NullPointerException.class, () -> stream.onKey(null));
  }

  @Test
  void onKeyWithNull() {
    stream.addToStream(-5, new DummyStreamRecord());
    assertThrows(NullPointerException.class, () -> stream.onKey(null));
  }

  @Test
  void onKeyWithNonExistentKeyOnEmptyStream() {
    assertTrue(stream.onKey(6).isEmpty());
  }

  @Test
  void onKeyWithNonExistentKey() {
    stream.addToStream(9, new DummyStreamRecord());
    assertTrue(stream.onKey(1).isEmpty());
  }

  @Test
  void onKeyOnStreamWithOnlyThatKey() {
    stream.addToStream(13, new DummyStreamRecord());
    assertInstanceOf(Stream.class, stream.onKey(13));
  }

  @Test
  void onKeyOnStreamWithMultipleKeys() {
    stream.addToStream(-2, new DummyStreamRecord());
    stream.addToStream(8, new DummyStreamRecord());
    assertInstanceOf(Stream.class, stream.onKey(8));
  }
}
