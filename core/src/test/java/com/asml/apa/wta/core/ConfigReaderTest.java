package com.asml.apa.wta.core;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.asml.apa.wta.core.config.RuntimeConfig;
import com.asml.apa.wta.core.utils.WtaUtils;
import java.util.HashMap;
import org.junit.jupiter.api.Test;

public class ConfigReaderTest {
  @Test
  void readsConfigFileCorrectly() {
    RuntimeConfig cr = WtaUtils.readConfig("src/test/resources/testConfig.json");
    assertThat(cr.getAuthor()).isEqualTo("Test Name");
    assertThat(cr.getDomain()).isEqualTo("Test Domain");
    assertThat(cr.getDescription()).isEqualTo("Test Description");
    assertThat(cr.getEvents()).isEqualTo(new HashMap<String, String>() {
      {
        put("f1", "v1");
        put("f2", "v2");
      }
    });
  }

  @Test
  void readsConfigFileWhereTheDescriptionIsNotThere() {
    RuntimeConfig cr = WtaUtils.readConfig("src/test/resources/testConfigNoDesc.json");
    assertThat(cr.getAuthor()).isEqualTo("Test Name");
    assertThat(cr.getDomain()).isEqualTo("Test Domain");
    assertThat(cr.getDescription()).isEqualTo("");
    assertThat(cr.getEvents()).isEqualTo(new HashMap<String, String>() {
      {
        put("f1", "v1");
        put("f2", "v2");
      }
    });
  }

  @Test
  void readsConfigFileWhereTheEventsAreNotThere() {
    RuntimeConfig cr = WtaUtils.readConfig("src/test/resources/testConfigNoEvents.json");
    assertThat(cr.getAuthor()).isEqualTo("Test Name");
    assertThat(cr.getDomain()).isEqualTo("Test Domain");
    assertThat(cr.getDescription()).isEqualTo("Test Description");
    assertThat(cr.getEvents()).isEqualTo(new HashMap<String, String>());
  }

  @Test
  void readsConfigFileWhereTheAuthorIsNotThere() {
    assertThatThrownBy(() -> {
          WtaUtils.readConfig("src/test/resources/testConfigInvalid.json");
        })
        .isInstanceOf(IllegalArgumentException.class);
  }
}
