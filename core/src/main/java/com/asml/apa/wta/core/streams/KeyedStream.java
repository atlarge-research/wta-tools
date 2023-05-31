package com.asml.apa.wta.core.streams;

import java.io.Serializable;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import lombok.Getter;
import lombok.NonNull;

/**
 * Keyed stream.
 *
 * @param <K> the key
 * @param <V> the class to hold, to extend {@link java.io.Serializable}
 * @author Atour Mousavi Gourabi
 * @since 1.0.0
 */
@Getter
public class KeyedStream<K, V extends Serializable> {

  private final Map<K, Stream<V>> streams = new ConcurrentHashMap<>();

  /**
   * Add to the keyed stream.
   *
   * @param key the record key
   * @param record the record
   * @author Atour Mousavi Gourabi
   * @since 1.0.0
   */
  public void addToStream(K key, @NonNull V record) {
    if (streams.containsKey(key)) {
      streams.get(key).addToStream(record);
    } else {
      streams.put(key, new Stream<>(record));
    }
  }

  /**
   * Get the message stream at a given key.
   *
   * @param key the key
   * @return the stream of objects with the provided key
   * @author Atour Mousavi Gourabi
   * @since 1.0.0
   */
  public Stream<V> onKey(K key) {
    streams.putIfAbsent(key, new Stream<>());
    return streams.get(key);
  }
}
