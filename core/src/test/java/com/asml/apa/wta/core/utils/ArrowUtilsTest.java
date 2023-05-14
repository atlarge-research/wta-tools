package com.asml.apa.wta.core.utils;

import com.asml.apa.wta.core.model.Resource;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class ArrowUtilsTest {

    private Resource resource;
    private ArrowUtils utils;
    List<Resource> resources;
    @BeforeEach
    void init(){
        var resourceBuilder = Resource.builder().id(1).type("test").os("test os").details("None").diskSpace(2).numResources(4.0).memory(8).networkSpeed(16).procModel("test model");
        resource = resourceBuilder.build();
        resources = new ArrayList<>();
        utils = new ArrowUtils(new File("."));
    }

    @Test
    void readResourceTest() {
        resources.add(resource);
        utils.readResource(resource);
        assertThat(resources).isEqualTo(utils.getResources());
    }

    /*
    @Test
    void writeResourceToFileTest() {
        resources.add(resource);
        utils.readResource(resource);
        try {
            utils.writeResourceToFile("test");
        }catch (Exception e){
            System.out.println(e.toString());
        }
    }
    
     */


}