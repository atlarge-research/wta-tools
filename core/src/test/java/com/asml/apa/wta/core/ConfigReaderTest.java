package com.asml.apa.wta.core;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

public class ConfigReaderTest {
  @Test
  public void goodWeather() {
    ConfigReader cr = new ConfigReader("src/test/testConfig.json");
    assertEquals(cr.getAuthor(), "Test Name");
    assertEquals(cr.getDomain(), "Test Domain");
    assertEquals(cr.getDescription(), "Test Description");
  }

  @Test
  public void testDescOptional() {
    ConfigReader cr = new ConfigReader("src/test/testConfigNoDesc.json");
    assertEquals(cr.getAuthor(), "Test Name");
    assertEquals(cr.getDomain(), "Test Domain");
    assertEquals(cr.getDescription(), "");
  }
}
