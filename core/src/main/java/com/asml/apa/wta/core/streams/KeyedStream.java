package com.asml.apa.wta.core.streams;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import lombok.NonNull;

/**
 * Keyed stream.
 *
 * @author Atour Mousavi Gourabi
 * @since 1.0.0
 */
public class KeyedStream<K, V extends StreamRecord<V>> {

  private final Map<K, Stream<V>> streams = new ConcurrentHashMap<>();

  /**
   * Add to the keyed stream.
   *
   * @param key the record key.
   * @param record the record.
   * @author Atour Mousavi Gourabi
   * @since 1.0.0
   */
  public void addToStream(@NonNull K key, @NonNull V record) {
    if (streams.containsKey(key)) {
      streams.get(key).addToStream(record);
    } else {
      streams.put(key, new Stream<>(record));
    }
  }

  /**
   * Consume the key.
   *
   * @param key the key
   * @return the stream of objects with the provided key
   * @author Atour Mousavi Gourabi
   * @since 1.0.0
   */
  public Stream<V> onKey(@NonNull K key) {
    if (streams.containsKey(key)) {
      return streams.get(key);
    } else {
      throw new RuntimeException("Key does not exist in the KeyedStream");
    }
  }
}
