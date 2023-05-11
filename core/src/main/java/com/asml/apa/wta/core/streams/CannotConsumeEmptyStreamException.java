package com.asml.apa.wta.core.streams;

/**
 * Exception that is thrown when an empty {@link com.asml.apa.wta.core.streams.Stream} is consumed.
 *
 * @author Atour Mousavi Gourabi
 * @since 1.0.0
 */
public class CannotConsumeEmptyStreamException extends Exception {

  private static final long serialVersionUID = -1700028230920829330L;

  /**
   * Constructs a {@link com.asml.apa.wta.core.streams.CannotConsumeEmptyStreamException}.
   *
   * @author Atour Mousavi Gourabi
   * @since 1.0.0
   */
  public CannotConsumeEmptyStreamException() {
    super("Empty streams cannot be consumed.");
  }
}
