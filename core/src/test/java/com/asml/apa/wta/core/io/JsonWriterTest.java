package com.asml.apa.wta.core.io;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import com.asml.apa.wta.core.model.Resource;
import java.io.BufferedOutputStream;
import java.io.IOException;
import org.junit.jupiter.api.Test;

class JsonWriterTest {

  @Test
  void close() throws IOException {
    OutputFile file = mock(DiskOutputFile.class);
    BufferedOutputStream stream = mock(BufferedOutputStream.class);
    when(file.open()).thenReturn(stream);
    JsonWriter<Resource> writer = new JsonWriter<>(file);
    writer.flush();
    verify(stream, times(1)).flush();
    verifyNoMoreInteractions(stream);
    writer.close();
  }

  @Test
  void flush() throws IOException {
    OutputFile file = mock(DiskOutputFile.class);
    BufferedOutputStream stream = mock(BufferedOutputStream.class);
    when(file.open()).thenReturn(stream);
    JsonWriter<Resource> writer = new JsonWriter<>(file);
    writer.close();
    verify(stream, times(1)).close();
    verifyNoMoreInteractions(stream);
  }
}
