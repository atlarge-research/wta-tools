```bash
#!/bin/bash

set -x

SGX=1 ./pal_loader ${JAVA_HOME}/bin/java \
        -cp '/ppml/trusted-big-data-ml/work/tpch-spark/target/scala-2.11/spark-tpc-h-queries_2.11-1.0.jar:/ppml/trusted-big-data-ml/work/tpch-spark/dbgen/*:/ppml/trusted-big-data-ml/work/bigdl-jar-with-dependencies.jar:/ppml/trusted-big-data-ml/work/spark-2.4.3/conf/:/ppml/trusted-big-data-ml/work/spark-2.4.3/jars/*' \
        -Xmx10g \
        -Dbigdl.mklNumThreads=1 \
        -XX:ActiveProcessorCount=24 \
        org.apache.spark.deploy.SparkSubmit \
        --master 'local[4]' \
        --conf spark.driver.port=10027 \
        --conf spark.scheduler.maxRegisteredResourcesWaitingTime=5000000 \
        --conf spark.worker.timeout=600 \
        --conf spark.starvation.timeout=250000 \
        --conf spark.rpc.askTimeout=600 \
        --conf spark.blockManager.port=10025 \
        --conf spark.driver.host=127.0.0.1 \
        --conf spark.driver.blockManager.port=10026 \
        --conf spark.io.compression.codec=lz4 \
        --conf spark.sql.shuffle.partitions=8 \
        --class main.scala.TpchQuery \
        --driver-memory 10G \
        /ppml/trusted-big-data-ml/work/tpch-spark/target/scala-2.11/spark-tpc-h-queries_2.11-1.0.jar \
        $1 $2 | tee spark.local.tpc.h.sgx.log
```
