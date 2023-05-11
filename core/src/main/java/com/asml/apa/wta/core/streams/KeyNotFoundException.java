package com.asml.apa.wta.core.streams;

/**
 * Exception that is thrown when a key could not be found in the {@link com.asml.apa.wta.core.streams.KeyedStream}
 * after {@link com.asml.apa.wta.core.streams.KeyedStream#onKey(Object)} was called.
 *
 * @author Atour Mousavi Gourabi
 * @since 1.0.0
 */
public class KeyNotFoundException extends RuntimeException {

  private static final long serialVersionUID = -3875011289743716105L;

  /**
   * Constructs a {@link KeyNotFoundException} with the key in the message.
   *
   * @param key the key the user tried to use
   * @author Atour Mousavi Gourabi
   * @since 1.0.0
   */
  public KeyNotFoundException(Object key) {
    super("Specified key " + key + " does not exist in the KeyedStream");
  }
}
