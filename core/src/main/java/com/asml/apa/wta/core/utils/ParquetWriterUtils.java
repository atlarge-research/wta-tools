package com.asml.apa.wta.core.utils;

import com.asml.apa.wta.core.model.Resource;
import com.asml.apa.wta.core.model.Task;
import com.asml.apa.wta.core.model.Workflow;
import com.asml.apa.wta.core.model.Workload;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

public class ParquetWriterUtils {

    private File path;
    private List<Resource> resources ;
    private List<Task> tasks;
    private List<Workflow> workflows;
    private List<Workload> workloads;
    private Schema resourceSchema;
    public ParquetWriterUtils(File path){
        resources = new ArrayList<>();
        tasks = new ArrayList<>();
        workflows = new ArrayList<>();
        workloads = new ArrayList<>();
        resourceSchema = SchemaBuilder.record("resource").namespace("com.asml.apa.wta.core.utils").fields()
                .name("id").type().longType().noDefault()
                .name("type").type().nullable().stringType().noDefault()
                .name("numResources").type().doubleType().noDefault()
                .name("procModel").type().nullable().stringType().noDefault()
                .name("memory").type().longType().noDefault()
                .name("diskSpace").type().longType().noDefault()
                .name("networkSpeed").type().longType().noDefault()
                .name("os").type().nullable().stringType().noDefault()
                .name("details").type().nullable().stringType().noDefault()
                .endRecord();
        this.path = path;
    }
    public List<Resource> getResources() {
        return resources;
    }
    public void readResource(Resource resource){
        resources.add(resource);
    }
    public void readTask(Task task){
        tasks.add(task);
    }
    public void readWorkflow(Workflow workflow){
        workflows.add(workflow);
    }
    public void readWorkload(Workload workload){
        workloads.add(workload);
    }
    public void writeToFile(){

    }
    public void writeResourceToFile(String resourceFileName) throws Exception {
        AvroUtils resourceWriter = new AvroUtils(resourceSchema,new File(path,"/resources/schema-1.0/"+resourceFileName+".parquet"));
        List<GenericRecord> resourceList = new ArrayList<>();
        for(Resource resource : resources){
            resourceList.add(this.convertResourceToRecord(resource));
        }
        resourceWriter.writeRecords(resourceList);
    }
    /*public static NullableVarCharHolder getNullableVarCharHolder(ArrowBuf buf, String s){
        NullableVarCharHolder vch = new NullableVarCharHolder();
        byte[] b = s.getBytes(Charsets.UTF_8);
        vch.start = 0;
        vch.end = b.length;
        vch.buffer = buf.reallocIfNeeded(b.length);
        vch.buffer.setBytes(0, b);
        vch.isSet = 1;
        return vch;
    }

     */
    private GenericRecord convertResourceToRecord(Resource resource){
        GenericData.Record record = new GenericData.Record(resourceSchema);
        record.put("id",resource.getId());
        record.put("type",resource.getType());
        record.put("numResources",resource.getNumResources());
        record.put("procModel",resource.getProcModel());
        record.put("memory",resource.getMemory());
        record.put("diskSpace",resource.getDiskSpace());
        record.put("networkSpeed",resource.getNetworkSpeed());
        record.put("os",resource.getOs());
        record.put("details",resource.getDetails());
        return record;
    }
}
