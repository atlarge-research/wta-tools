package com.asml.apa.wta.core.streams;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

class StreamTest {

  @Test
  void constructStreamAsNull() {
    assertThrows(NullPointerException.class, () -> new Stream<>(null));
  }

  @Test
  void headWithValidStream() {
    IntegerStreamRecord record = new IntegerStreamRecord(1);
    Stream<IntegerStreamRecord> stream = new Stream<>(record);
    assertEquals(stream.head(), record);
  }

  @Test
  void headWithEmptyStream() {
    IntegerStreamRecord record = new IntegerStreamRecord(1);
    Stream<IntegerStreamRecord> stream = new Stream<>(record);
    stream.head();
    assertThrows(NullPointerException.class, stream::head);
  }

  @Test
  void addNullToStream() {
    Stream<DummyStreamRecord> stream = new Stream<>(new DummyStreamRecord());
    assertThrows(NullPointerException.class, () -> stream.addToStream(null));
  }

  @Test
  void addValidToStream() {
    Stream<DummyStreamRecord> stream = new Stream<>(new DummyStreamRecord());
    assertDoesNotThrow(() -> stream.addToStream(new DummyStreamRecord()));
  }

  @Test
  void addToStreamCircular() {
    DummyStreamRecord record = new DummyStreamRecord();
    Stream<DummyStreamRecord> stream = new Stream<>(record);
    assertDoesNotThrow(() -> stream.addToStream(record));
  }

  @Test
  void mapWithNullFunction() {
    Stream<DummyStreamRecord> stream = new Stream<>(new DummyStreamRecord());
    assertThrows(NullPointerException.class, () -> stream.map(null));
  }

  @Test
  void mapNonCircularStream() {
    Stream<IntegerStreamRecord> nats = new Stream<>(new IntegerStreamRecord(0));
    for (int i = 1; i <= 10; i++) {
      nats.addToStream(new IntegerStreamRecord(i));
    }
    Stream<IntegerStreamRecord> pos = nats.map((n) -> new IntegerStreamRecord(n.getField() + 1));
    assertEquals(pos.head().getField(), 1);
  }

  @Test
  void mapCircularStream() {
    DummyStreamRecord record = new DummyStreamRecord();
    Stream<DummyStreamRecord> stream = new Stream<>(record);
    stream.addToStream(record);
    DummyStreamRecord record2 = new DummyStreamRecord();
    assertEquals(stream.map((n) -> record2).head(), record2);
  }

  @Test
  void foldLeft() {
    Stream<IntegerStreamRecord> nats = new Stream<>(new IntegerStreamRecord(0));
    for (int i = 1; i <= 10; i++) {
      nats.addToStream(new IntegerStreamRecord(i));
    }
    assertEquals(55, nats.foldLeft(0, (i, r) -> r.getField() + i));
  }

  @Test
  void filter() {
    DummyStreamRecord record = new DummyStreamRecord();
    Stream<DummyStreamRecord> stream = new Stream<>(new DummyStreamRecord());
    stream.addToStream(record);
    assertEquals(stream.filter((n) -> n == record).head(), record);
  }

  @Test
  void filterCircularStream() {
    DummyStreamRecord record = new DummyStreamRecord();
    Stream<DummyStreamRecord> stream = new Stream<>(record);
    stream.addToStream(record);
    assertTrue(stream.filter((n) -> false).isEmpty());
  }
}