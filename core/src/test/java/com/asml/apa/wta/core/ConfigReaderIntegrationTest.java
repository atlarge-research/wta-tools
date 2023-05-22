package com.asml.apa.wta.core;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.asml.apa.wta.core.config.RuntimeConfig;
import com.asml.apa.wta.core.utils.WtaUtils;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

class ConfigReaderIntegrationTest {

  @Test
  void readsConfigFileCorrectly() {
    RuntimeConfig cr = WtaUtils.readConfig("src/test/resources/testConfig.json");
    assertThat(cr.getAuthor()).isEqualTo("Test Name");
    assertThat(cr.getDomain()).isEqualTo("Test Domain");
    assertThat(cr.getDescription()).isEqualTo("Test Description");
    Map<String, String> map = new HashMap<>();
    map.put("f1", "v1");
    map.put("f2", "v2");
    assertThat(cr.getEvents()).isEqualTo(map);
    assertThat(cr.getLogLevel()).isEqualTo("INFO");
  }

  @Test
  void readsConfigFileWhereTheDescriptionIsNotThere() {
    RuntimeConfig cr = WtaUtils.readConfig("src/test/resources/testConfigNoDesc.json");
    assertThat(cr.getAuthor()).isEqualTo("Test Name");
    assertThat(cr.getDomain()).isEqualTo("Test Domain");
    assertThat(cr.getDescription()).isEqualTo("");
    Map<String, String> map = new HashMap<>();
    map.put("f1", "v1");
    map.put("f2", "v2");
    assertThat(cr.getEvents()).isEqualTo(map);
    assertThat(cr.getLogLevel()).isEqualTo("INFO");
  }

  @Test
  void readsConfigFileWhereTheEventsAreNotThere() {
    RuntimeConfig cr = WtaUtils.readConfig("src/test/resources/testConfigNoEvents.json");
    assertThat(cr.getAuthor()).isEqualTo("Test Name");
    assertThat(cr.getDomain()).isEqualTo("Test Domain");
    assertThat(cr.getDescription()).isEqualTo("Test Description");
    assertThat(cr.getEvents()).isEqualTo(new HashMap<>());
  }

  @Test
  void readsConfigFileWhereTheAuthorIsNotThere() {
    assertThatThrownBy(() -> WtaUtils.readConfig("src/test/resources/testConfigInvalid.json"))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void readsConfigFileWhereLogSettingIsNotThere() {
    RuntimeConfig cr = WtaUtils.readConfig("src/test/resources/testConfigNoLogSettings.json");
    assertThat(cr.getAuthor()).isEqualTo("Test Name");
    assertThat(cr.getDomain()).isEqualTo("Test Domain");
    assertThat(cr.getDescription()).isEqualTo("Test Description");
    Map<String, String> map = new HashMap<>();
    map.put("f1", "v1");
    map.put("f2", "v2");
    assertThat(cr.getEvents()).isEqualTo(map);
    assertThat(cr.getLogLevel()).isEqualTo("ERROR");
  }
}
