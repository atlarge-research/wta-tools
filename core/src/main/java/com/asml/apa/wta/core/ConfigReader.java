package com.asml.apa.wta.core;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.File;

public class ConfigReader {
  String author;
  String domain;

  public ConfigReader() {
    try {
      // create ObjectMapper instance
      ObjectMapper mapper = new ObjectMapper();

      // read JSON file into a JsonNode object
      JsonNode rootNode = mapper.readTree(new File("config.json"));

      // extract the author and domain fields from the JsonNode object
      String author = rootNode.get("author").asText();
      String domain = rootNode.get("domain").asText();

      this.author = author;
      this.domain = domain;

    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
