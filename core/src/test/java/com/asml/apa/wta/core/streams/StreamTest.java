package com.asml.apa.wta.core.streams;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

class StreamTest {

  Stream<IntegerStreamRecord> generateNaturalNumbersUpToAndIncluding(int n) {
    Stream<IntegerStreamRecord> nats = new Stream<>(new IntegerStreamRecord(0));
    for (int i = 1; i <= n; i++) {
      nats.addToStream(new IntegerStreamRecord(i));
    }
    return nats;
  }

  @Test
  void constructStreamAsNull() {
    assertThrows(NullPointerException.class, () -> new Stream<>(null));
  }

  @Test
  void constructsEmptyStream() {
    assertDoesNotThrow(() -> new Stream<>());
  }

  @Test
  void isEmptyOnEmptyStream() {
    Stream<DummyStreamRecord> emptyStream = new Stream<>();
    assertTrue(emptyStream.isEmpty());
  }

  @Test
  void isEmpty() {
    Stream<IntegerStreamRecord> emptyStream = new Stream<>(new IntegerStreamRecord(1));
    assertFalse(emptyStream.isEmpty());
  }

  @Test
  void headWithValidStream() {
    IntegerStreamRecord record = new IntegerStreamRecord(1);
    Stream<IntegerStreamRecord> stream = new Stream<>(record);
    assertEquals(stream.head(), record);
  }

  @Test
  void headVerifyConsumedStream() {
    Stream<DummyStreamRecord> stream = new Stream<>(new DummyStreamRecord());
    stream.head();
    assertTrue(stream.isEmpty());
  }

  @Test
  void headWithEmptyStream() {
    Stream<IntegerStreamRecord> stream = new Stream<>();
    assertThrows(NullPointerException.class, stream::head);
  }

  @Test
  void safeHeadWithValidStream() throws CannotConsumeEmptyStreamException {
    Stream<IntegerStreamRecord> stream = new Stream<>(new IntegerStreamRecord(1));
    assertEquals(1, stream.safeHead().getField());
  }

  @Test
  void safeHeadVerifyConsumedStream() throws CannotConsumeEmptyStreamException {
    Stream<DummyStreamRecord> stream = new Stream<>(new DummyStreamRecord());
    stream.safeHead();
    assertTrue(stream.isEmpty());
  }

  @Test
  void safeHeadWithEmptyStream() {
    Stream<IntegerStreamRecord> stream = new Stream<>();
    assertThrows(CannotConsumeEmptyStreamException.class, stream::safeHead);
  }

  @Test
  void addNullToStream() {
    Stream<DummyStreamRecord> stream = new Stream<>(new DummyStreamRecord());
    assertThrows(NullPointerException.class, () -> stream.addToStream(null));
  }

  @Test
  void addValidToStream() {
    Stream<DummyStreamRecord> stream = new Stream<>(new DummyStreamRecord());
    assertNotEquals(stream.addToStream(new DummyStreamRecord()).head(), stream.head());
  }

  @Test
  void addToStreamCircular() {
    DummyStreamRecord record = new DummyStreamRecord();
    Stream<DummyStreamRecord> stream = new Stream<>(record);
    assertDoesNotThrow(() -> stream.addToStream(record));
  }

  @Test
  void safeAddToStream() {
    DummyStreamRecord record = new DummyStreamRecord();
    Stream<DummyStreamRecord> stream = new Stream<>();
    assertEquals(record, stream.safeAddToStream(record).head());
  }

  @Test
  void safeAddNullToStream() {
    Stream<DummyStreamRecord> stream = new Stream<>(new DummyStreamRecord());
    assertThrows(NullPointerException.class, () -> stream.safeAddToStream(null));
  }

  @Test
  void map() {
    Stream<IntegerStreamRecord> nats = generateNaturalNumbersUpToAndIncluding(10);
    Stream<IntegerStreamRecord> pos = nats.map((n) -> new IntegerStreamRecord(n.getField() + 1));
    assertEquals(pos.head().getField(), 1);
  }

  @Test
  void mapVerifyConsumedStream() {
    Stream<IntegerStreamRecord> nats = generateNaturalNumbersUpToAndIncluding(10);
    nats.map((n) -> n);
    assertTrue(nats.isEmpty());
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
  void mapWithNullFunction() {
    Stream<DummyStreamRecord> stream = new Stream<>(new DummyStreamRecord());
    assertThrows(NullPointerException.class, () -> stream.map(null));
  }

  @Test
  void mapNaturalNumbersToNegative() {
    Stream<IntegerStreamRecord> nats = generateNaturalNumbersUpToAndIncluding(3);
    Stream<IntegerStreamRecord> negs = nats.map((n) -> new IntegerStreamRecord(-n.getField()));
    assertFalse(negs.isEmpty());
    assertEquals(negs.head().getField(), 0);
    assertEquals(negs.head().getField(), -1);
    assertEquals(negs.head().getField(), -2);
    assertEquals(negs.head().getField(), -3);
  }

  @Test
  void foldLeft() {
    Stream<IntegerStreamRecord> nats = generateNaturalNumbersUpToAndIncluding(10);
    assertEquals(55, nats.foldLeft(0, (i, r) -> r.getField() + i));
  }

  @Test
  void foldLeftWithNullOp() {
    Stream<IntegerStreamRecord> nats = generateNaturalNumbersUpToAndIncluding(10);
    assertThrows(NullPointerException.class, () -> nats.foldLeft(0, null));
  }

  @Test
  void foldLeftVerifyConsumedStream() {
    Stream<IntegerStreamRecord> nats = generateNaturalNumbersUpToAndIncluding(10);
    nats.foldLeft(0, (i, r) -> r.getField() + i);
    assertTrue(nats.isEmpty());
  }

  @Test
  void filter() {
    DummyStreamRecord record = new DummyStreamRecord();
    Stream<DummyStreamRecord> stream = new Stream<>(new DummyStreamRecord());
    stream.addToStream(record);
    assertEquals(stream.filter((n) -> n == record).head(), record);
  }

  @Test
  void filterVerifyConsumedStream() {
    Stream<IntegerStreamRecord> nats = generateNaturalNumbersUpToAndIncluding(10);
    nats.filter((n) -> n.getField() > 5);
    assertTrue(nats.isEmpty());
  }

  @Test
  void filterNaturalNumbersOverFive() {
    Stream<IntegerStreamRecord> nats = generateNaturalNumbersUpToAndIncluding(10);
    assertEquals(nats.filter((n) -> n.getField() > 5).head().getField(), 6);
  }

  @Test
  void filterCircularStream() {
    DummyStreamRecord record = new DummyStreamRecord();
    Stream<DummyStreamRecord> stream = new Stream<>(record);
    stream.addToStream(record);
    assertTrue(stream.filter((n) -> false).isEmpty());
  }

  @Test
  void filterOnEmptyStream() {
    Stream<DummyStreamRecord> stream = new Stream<>();
    assertTrue(stream.filter((n) -> true).isEmpty());
  }

  @Test
  void filterWithNullPredicate() {
    Stream<DummyStreamRecord> stream = new Stream<>(new DummyStreamRecord());
    assertThrows(NullPointerException.class, () -> stream.filter(null));
  }
}
