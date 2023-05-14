package com.asml.apa.wta.core.exceptions;

import lombok.NoArgsConstructor;

/**
 * Exception that is thrown when a {@link com.asml.apa.wta.core.streams.Stream} fails serialization.
 *
 * @author Atour Mousavi Gourabi
 * @since 1.0.0
 */
@NoArgsConstructor
public class FailedToSerializeStreamException extends StreamSerializationException {

  private static final long serialVersionUID = 8275510433682074736L;
}
