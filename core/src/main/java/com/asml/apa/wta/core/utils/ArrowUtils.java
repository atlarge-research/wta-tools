package com.asml.apa.wta.core.utils;

import com.asml.apa.wta.core.model.Resource;
import com.asml.apa.wta.core.model.Task;
import com.asml.apa.wta.core.model.Workflow;
import com.asml.apa.wta.core.model.Workload;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.parquet.avro.AvroParquetWriter;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

public class ArrowUtils {

    private File path;
    private List<Resource> resources ;
    private List<Task> tasks;
    private List<Workflow> workflows;
    private List<Workload> workloads;
    private Schema resourceSchema;
    public ArrowUtils(File path){
        resources = new ArrayList<>();
        tasks = new ArrayList<>();
        workflows = new ArrayList<>();
        workloads = new ArrayList<>();
        resourceSchema = SchemaBuilder.record("resource").namespace("com.asml.apa.wta.core.utils").fields().name("id")
                .type().nullable().longType().noDefault().endRecord();
        this.path = path;
    }
    public void readResource(Resource resource){
        resources.add(resource);
    }
    public void readTask(Task task){
        tasks.add(task);
    }

    public List<Resource> getResources() {
        return resources;
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
        ParquetWriterUtils resourceWriter = new ParquetWriterUtils(resourceSchema,new File(path,"/resources/schema-1.0/"+resourceFileName+".parquet"));
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
        return record;
    }
}
