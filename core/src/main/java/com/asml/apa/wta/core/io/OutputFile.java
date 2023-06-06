package com.asml.apa.wta.core.io;

import java.io.Flushable;
import java.io.IOException;

public interface OutputFile extends Appendable, AutoCloseable, Flushable {
  OutputFile resolve(String path);

  OutputFile open() throws IOException;

  void clearDirectory() throws IOException;
}
