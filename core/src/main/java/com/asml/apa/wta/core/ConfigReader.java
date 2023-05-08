package com.asml.apa.wta.core;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.File;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class ConfigReader {
  private String author;
  private String domain;
  private String description;

  public ConfigReader() {
    try {
      // create ObjectMapper instance
      ObjectMapper mapper = new ObjectMapper();

      // read JSON file into a JsonNode object
      JsonNode rootNode = mapper.readTree(new File("config.json"));

      // extract the author, domain, and description fields from the JsonNode object
      JsonNode workloadNode = rootNode.get("workloadSettings");
      this.author = workloadNode.get("author").asText();
      this.domain = workloadNode.get("domain").asText();
      this.description = workloadNode.has("description")
          ? workloadNode.get("description").asText()
          : "";

    } catch (Exception e) {
      System.err.println("A mandatory field is missing");
    }
  }
}
