package com.asml.apa.wta.core.exceptions;

/**
 * Exception that is thrown when a bash command fails.
 *
 * @author Lohithsai Yadala Chanchu
 * @since 1.0.0
 */
public class BashCommandExecutionException extends RuntimeException {
  public BashCommandExecutionException(String message, Throwable cause) {
    super(message, cause);
  }
}
