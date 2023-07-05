package com.asml.apa.wta.core.exception;

import lombok.NoArgsConstructor;

/**
 * Exception that is thrown when a {@link com.asml.apa.wta.core.stream.Stream} fails serialization.
 *
 * @author Atour Mousavi Gourabi
 * @since 1.0.0
 */
@NoArgsConstructor
public class FailedToDeserializeStreamException extends StreamSerializationException {

  private static final long serialVersionUID = -6392668088940006154L;
}
