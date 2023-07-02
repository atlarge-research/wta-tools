package com.asml.apa.wta.core.stream;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiFunction;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

/**
 * Keyed stream.
 *
 * @param <K> the key
 * @param <V> the class to hold, to extend {@link java.io.Serializable}
 * @author Atour Mousavi Gourabi
 * @author Henry Page
 * @since 1.0.0
 */
@Slf4j
public class KeyedStream<K, V extends Serializable> {

  private final Map<K, Stream<V>> streams = new ConcurrentHashMap<>();

  /**
   * Add to the keyed stream.
   *
   * @param key       record key
   * @param record    record
   */
  public void addToStream(K key, @NonNull V record) {
    log.trace("Adding object to stream");
    if (streams.containsKey(key)) {
      streams.get(key).addToStream(record);
    } else {
      streams.put(key, new Stream<>(record));
    }
  }

  /**
   * Drops the elements associated to the given key from the {@link KeyedStream}.
   *
   * @param key       key to remove the elements from.
   */
  public void dropKey(K key) {
    streams.remove(key);
  }

  /**
   * Performs the mapping operation over the {@link KeyedStream} per key.
   * Consumes the {@link KeyedStream}.
   *
   * @param mapper    mapping function, takes in the key and value of the element to map
   * @param <R>       type parameter for the mapper's return type
   * @return          result of the mapping operation
   */
  public <R> List<R> mapKeyList(@NonNull BiFunction<K, Stream<V>, R> mapper) {
    List<R> stream = new ArrayList<>();
    for (K key : streams.keySet()) {
      stream.add(mapper.apply(key, streams.get(key)));
    }
    return stream;
  }

  /**
   * Get the message stream at a given key.
   *
   * @param key       key
   * @return          stream of objects with the provided key
   */
  public Stream<V> onKey(K key) {
    log.trace("Requested stream with key");
    streams.putIfAbsent(key, new Stream<>());
    return streams.get(key).copy();
  }
}
