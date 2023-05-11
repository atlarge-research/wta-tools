package com.asml.apa.wta.core.streams;

/**
 * Dummy StreamRecord implementation for core module testing.
 *
 * @author Atour Mousavi Gourabi
 */
public class DummyStreamRecord implements StreamRecord<DummyStreamRecord> {

  private DummyStreamRecord next;

  @Override
  public DummyStreamRecord setNext(DummyStreamRecord next) {
    return this.next = next;
  }

  @Override
  public DummyStreamRecord getNext() {
    return next;
  }
}
