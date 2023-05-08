package com.asml.apa.wta.core;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.File;

public class ConfigReader {
  String author;
  String domain;
  String description;

  public ConfigReader() {
    try {
      // create ObjectMapper instance
      ObjectMapper mapper = new ObjectMapper();

      // read JSON file into a JsonNode object
      JsonNode rootNode = mapper.readTree(new File("config.json"));

      // extract the author, domain, and description fields from the JsonNode object
      this.author = rootNode.get("author").asText();
      this.domain = rootNode.get("domain").asText();
      String description = rootNode.has("description") ? rootNode.get("description").asText() : "";


    } catch (Exception e) {
      System.out.println("Config file error");
    }
  }
}
