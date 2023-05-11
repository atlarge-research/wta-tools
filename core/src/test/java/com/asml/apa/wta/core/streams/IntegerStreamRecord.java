package com.asml.apa.wta.core.streams;

import lombok.Getter;
import lombok.Setter;

/**
 * Dummy StreamRecord implementation for core module testing.
 *
 * @author Atour Mousavi Gourabi
 */
public class IntegerStreamRecord implements StreamRecord<IntegerStreamRecord> {

  private IntegerStreamRecord next;

  @Getter
  private final int field;

  public IntegerStreamRecord(int n) {
    field = n;
  }

  @Override
  public IntegerStreamRecord setNext(IntegerStreamRecord next) {
    return this.next = next;
  }

  @Override
  public IntegerStreamRecord getNext() {
    return next;
  }
}
