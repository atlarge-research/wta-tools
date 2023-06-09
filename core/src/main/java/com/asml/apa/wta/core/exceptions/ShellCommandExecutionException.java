package com.asml.apa.wta.core.exceptions;

/**
 * Exception that is thrown when a shell command fails.
 *
 * @author Lohithsai Yadala Chanchu
 * @since 1.0.0
 */
public class ShellCommandExecutionException extends RuntimeException {

  private static final long serialVersionUID = -2222274543443460090L;

  public ShellCommandExecutionException(String message, Throwable cause) {
    super(message, cause);
  }
}
