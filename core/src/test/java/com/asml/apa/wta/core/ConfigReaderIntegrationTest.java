package com.asml.apa.wta.core;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.asml.apa.wta.core.config.RuntimeConfig;
import com.asml.apa.wta.core.model.enums.Domain;
import com.asml.apa.wta.core.utils.WtaUtils;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

class ConfigReaderIntegrationTest {

  @Test
  void readsConfigFileCorrectly() {
    RuntimeConfig cr = WtaUtils.readConfig("src/test/resources/testConfig.json");
    assertThat(cr.getAuthors()).isEqualTo(new String[] {"Test Name"});
    assertThat(cr.getDomain()).isEqualTo(Domain.SCIENTIFIC);
    assertThat(cr.getDescription()).isEqualTo("Test Description");
    assertThat(cr.isStageLevel()).isEqualTo(true);
    Map<String, String> map = new HashMap<>();
    map.put("f1", "v1");
    map.put("f2", "v2");
    assertThat(cr.getEvents()).isEqualTo(map);
    assertThat(cr.getLogLevel()).isEqualTo("INFO");
  }

  @Test
  void readsConfigFileWhereTheDescriptionIsNotThere() {
    RuntimeConfig cr = WtaUtils.readConfig("src/test/resources/testConfigNoDesc.json");
    assertThat(cr.getAuthors()).isEqualTo(new String[] {"Test Name"});
    assertThat(cr.getDomain()).isEqualTo(Domain.ENGINEERING);
    assertThat(cr.getDescription()).isEqualTo("");
    assertThat(cr.isStageLevel()).isEqualTo(true);
    Map<String, String> map = new HashMap<>();
    map.put("f1", "v1");
    map.put("f2", "v2");
    assertThat(cr.getEvents()).isEqualTo(map);
    assertThat(cr.getLogLevel()).isEqualTo("INFO");
  }

  @Test
  void readsConfigFileWhereTheEventsAreNotThere() {
    RuntimeConfig cr = WtaUtils.readConfig("src/test/resources/testConfigNoEvents.json");
    assertThat(cr.getAuthors()).isEqualTo(new String[] {"Test Name"});
    assertThat(cr.getDomain()).isEqualTo(Domain.INDUSTRIAL);
    assertThat(cr.getDescription()).isEqualTo("Test Description");
    assertThat(cr.getEvents()).isEqualTo(new HashMap<>());
    assertThat(cr.isStageLevel()).isEqualTo(true);
  }

  @Test
  void readsConfigFileWhereTheAuthorIsNotThere() {
    assertThatThrownBy(() -> WtaUtils.readConfig("src/test/resources/testConfigInvalid.json"))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void readsConfigFileWhereLogSettingIsNotThere() {
    RuntimeConfig cr = WtaUtils.readConfig("src/test/resources/testConfigNoLogSettings.json");
    assertThat(cr.getAuthors()).isEqualTo(new String[] {"Test Name"});
    assertThat(cr.getDomain()).isEqualTo(Domain.INDUSTRIAL);
    assertThat(cr.getDescription()).isEqualTo("Test Description");
    assertThat(cr.isStageLevel()).isEqualTo(true);
    Map<String, String> map = new HashMap<>();
    map.put("f1", "v1");
    map.put("f2", "v2");
    assertThat(cr.getEvents()).isEqualTo(map);
    assertThat(cr.getLogLevel()).isEqualTo("ERROR");
  }

  @Test
  void readsConfigFileWhereIsStageLevelIsNotThere() {
    RuntimeConfig cr = WtaUtils.readConfig("src/test/resources/testConfigNoIsStageLevel.json");
    assertThat(cr.getAuthors()).isEqualTo(new String[] {"Test Name"});
    assertThat(cr.getDomain()).isEqualTo(Domain.SCIENTIFIC);
    assertThat(cr.getDescription()).isEqualTo("Test Description");
    assertThat(cr.isStageLevel()).isEqualTo(false);
    Map<String, String> map = new HashMap<>();
    map.put("f1", "v1");
    map.put("f2", "v2");
    assertThat(cr.getEvents()).isEqualTo(map);
    assertThat(cr.getLogLevel()).isEqualTo("INFO");
  }
}
