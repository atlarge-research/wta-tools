#!/bin/bash
export SPARK_HOME=$SPARK_HOME
export MASTER=local[4]
export ANALYTICS_ZOO_HOME=$ANALYTICS_ZOO_HOME
export ANALYTICS_ZOO_JAR=`find ${ANALYTICS_ZOO_HOME}/lib -type f -name "analytics-zoo*jar-with-dependencies.jar"`
export ANALYTICS_ZOO_PYZIP=`find ${ANALYTICS_ZOO_HOME}/lib -type f -name "analytics-zoo*python-api.zip"`
export ANALYTICS_ZOO_CONF=${ANALYTICS_ZOO_HOME}/conf/spark-analytics-zoo.conf
export PYTHONPATH=${ANALYTICS_ZOO_PYZIP}:$PYTHONPATH

chmod +x ${ZOO_TUTORIALS}/keras/ipynb2py.sh

set -e

echo "#1 2.1-a-first-look-at-a-neural-network.ipynb"
#timer
start=$(date "+%s")
${ZOO_TUTORIALS}/keras/ipynb2py.sh ${ZOO_TUTORIALS}/keras/2.1-a-first-look-at-a-neural-network

sed "s/nb_epoch=5/nb_epoch=5/g" ${ZOO_TUTORIALS}/keras/2.1-a-first-look-at-a-neural-network.py >${ZOO_TUTORIALS}/keras/tmp_test.py
sed -i "s/zoo.pipeline.api.keras.datasets/tensorflow.keras.datasets/g" ${ZOO_TUTORIALS}/keras/tmp_test.py
${SPARK_HOME}/bin/spark-submit \
        --master ${MASTER} \
        --driver-cores 2  \
        --driver-memory 12g  \
        --total-executor-cores 2  \
        --executor-cores 2  \
        --executor-memory 12g \
        --conf spark.akka.frameSize=64 \
        --py-files ${ANALYTICS_ZOO_PYZIP},${ZOO_TUTORIALS}/keras/tmp_test.py \
        --properties-file ${ANALYTICS_ZOO_CONF} \
        --jars ${ANALYTICS_ZOO_JAR} \
        --conf spark.driver.extraClassPath=${ANALYTICS_ZOO_JAR} \
        --conf spark.executor.extraClassPath=${ANALYTICS_ZOO_JAR} \
        ${ZOO_TUTORIALS}/keras/tmp_test.py
now=$(date "+%s")
time1=$((now-start))
rm ${ZOO_TUTORIALS}/keras/tmp_test.py
echo "#1 2.1-a-first-look-at-a-neural-network.ipynb"

echo "#2 3.5-classifying-movie-reviews"
#timer
start=$(date "+%s")
${ZOO_TUTORIALS}/keras/ipynb2py.sh ${ZOO_TUTORIALS}/keras/3.5-classifying-movie-reviews
sed "s/nb_epoch=20/nb_epoch=4/g" ${ZOO_TUTORIALS}/keras/3.5-classifying-movie-reviews.py >${ZOO_TUTORIALS}/keras/tmp_test.py
sed -i "s/get_ipython()/#/g" ${ZOO_TUTORIALS}/keras/tmp_test.py
sed -i "s/import matplotlib.pyplot as plt/import matplotlib/g" ${ZOO_TUTORIALS}/keras/tmp_test.py
sed -i '/^import matplotlib$/a\matplotlib.use('\''Agg'\'')' ${ZOO_TUTORIALS}/keras/tmp_test.py
sed -i '/^matplotlib.use('\''Agg'\'')$/a\from matplotlib import pyplot as plt' ${ZOO_TUTORIALS}/keras/tmp_test.py
${SPARK_HOME}/bin/spark-submit \
        --master ${MASTER} \
        --driver-cores 2  \
        --driver-memory 17g  \
        --total-executor-cores 2  \
        --executor-cores 2  \
        --executor-memory 17g \
        --conf spark.akka.frameSize=64 \
        --py-files ${ANALYTICS_ZOO_PYZIP},${ZOO_TUTORIALS}//keras/tmp_test.py  \
        --properties-file ${ANALYTICS_ZOO_CONF} \
        --jars ${ANALYTICS_ZOO_JAR} \
        --conf spark.driver.extraClassPath=${ANALYTICS_ZOO_JAR} \
        --conf spark.executor.extraClassPath=${ANALYTICS_ZOO_JAR} \
        ${ZOO_TUTORIALS}/keras/tmp_test.py
now=$(date "+%s")
time2=$((now-start))
rm ${ZOO_TUTORIALS}/keras/tmp_test.py
echo "#2 3.5-classifying-movie-reviews used:$time2 seconds"

echo "#3 3.6-classifying-newswires"
#timer
start=$(date "+%s")
${ZOO_TUTORIALS}/keras/ipynb2py.sh ${ZOO_TUTORIALS}/keras/3.6-classifying-newswires
sed "s/nb_epoch=20/nb_epoch=8/g" ${ZOO_TUTORIALS}/keras/3.6-classifying-newswires.py >${ZOO_TUTORIALS}/keras/tmp_test.py
sed -i "s/get_ipython()/#/g" ${ZOO_TUTORIALS}/keras/tmp_test.py
sed -i "s/import matplotlib.pyplot as plt/import matplotlib/g" ${ZOO_TUTORIALS}/keras/tmp_test.py
sed -i '/^import matplotlib$/a\matplotlib.use('\''Agg'\'')' ${ZOO_TUTORIALS}/keras/tmp_test.py
sed -i '/^matplotlib.use('\''Agg'\'')$/a\from matplotlib import pyplot as plt' ${ZOO_TUTORIALS}/keras/tmp_test.py
${SPARK_HOME}/bin/spark-submit \
        --master ${MASTER} \
        --driver-cores 2  \
        --driver-memory 8g  \
        --total-executor-cores 2  \
        --executor-cores 2  \
        --executor-memory 8g \
        --conf spark.akka.frameSize=64 \
        --py-files ${ANALYTICS_ZOO_PYZIP},${ZOO_TUTORIALS}/keras/tmp_test.py  \
        --properties-file ${ANALYTICS_ZOO_CONF} \
        --jars ${ANALYTICS_ZOO_JAR} \
        --conf spark.driver.extraClassPath=${ANALYTICS_ZOO_JAR} \
        --conf spark.executor.extraClassPath=${ANALYTICS_ZOO_JAR} \
        ${ZOO_TUTORIALS}/keras/tmp_test.py
now=$(date "+%s")
time3=$((now-start))
rm ${ZOO_TUTORIALS}/keras/tmp_test.py
echo "#3 3.6-classifying-newswires time used:$time3 seconds"

echo "#4 3.7-predicting-house-prices"
#timer
start=$(date "+%s")
${ZOO_TUTORIALS}/keras/ipynb2py.sh ${ZOO_TUTORIALS}/keras/3.7-predicting-house-prices
sed "s/num_nb_epoch = 50/num_nb_epoch=2/g" ${ZOO_TUTORIALS}/keras/3.7-predicting-house-prices.py >${ZOO_TUTORIALS}/keras/tmp_test.py
sed -i "s/num_epochs = 500/num_epochs = 10/g" ${ZOO_TUTORIALS}/keras/tmp_test.py
sed -i "s/nb_epoch=150/nb_epoch=5/g" ${ZOO_TUTORIALS}/keras/tmp_test.py
sed -i "s/get_ipython()/#/g" ${ZOO_TUTORIALS}/keras/tmp_test.py
sed -i "s/import matplotlib.pyplot as plt/import matplotlib/g" ${ZOO_TUTORIALS}/keras/tmp_test.py
sed -i '/^import matplotlib$/a\matplotlib.use('\''Agg'\'')' ${ZOO_TUTORIALS}/keras/tmp_test.py
sed -i '/^matplotlib.use('\''Agg'\'')$/a\from matplotlib import pyplot as plt' ${ZOO_TUTORIALS}/keras/tmp_test.py
${SPARK_HOME}/bin/spark-submit \
        --master ${MASTER} \
        --driver-cores 2  \
        --driver-memory 8g  \
        --total-executor-cores 2  \
        --executor-cores 2  \
        --executor-memory 8g \
        --conf spark.akka.frameSize=64 \
        --py-files ${ANALYTICS_ZOO_PYZIP},${ZOO_TUTORIALS}/keras/tmp_test.py  \
        --properties-file ${ANALYTICS_ZOO_CONF} \
        --jars ${ANALYTICS_ZOO_JAR} \
        --conf spark.driver.extraClassPath=${ANALYTICS_ZOO_JAR} \
        --conf spark.executor.extraClassPath=${ANALYTICS_ZOO_JAR} \
        ${ZOO_TUTORIALS}/keras/tmp_test.py
now=$(date "+%s")
time4=$((now-start))
rm ${ZOO_TUTORIALS}/keras/tmp_test.py
echo "#4 3.7-predicting-house-prices time used:$time4 seconds"

echo "#5 4.4-overfitting-and-underfitting"
#timer
start=$(date "+%s")
${ZOO_TUTORIALS}/keras/ipynb2py.sh ${ZOO_TUTORIALS}/keras/4.4-overfitting-and-underfitting
sed "s/nb_epoch=20/nb_epoch=1/g" ${ZOO_TUTORIALS}/keras/4.4-overfitting-and-underfitting.py >${ZOO_TUTORIALS}/keras/tmp_test.py
sed -i "s/get_ipython()/#/g" ${ZOO_TUTORIALS}/keras/tmp_test.py
sed -i "s/import matplotlib.pyplot as plt/import matplotlib/g" ${ZOO_TUTORIALS}/keras/tmp_test.py
sed -i '/^import matplotlib$/a\matplotlib.use('\''Agg'\'')' ${ZOO_TUTORIALS}/keras/tmp_test.py
sed -i '/^matplotlib.use('\''Agg'\'')$/a\from matplotlib import pyplot as plt' ${ZOO_TUTORIALS}/keras/tmp_test.py
${SPARK_HOME}/bin/spark-submit \
        --master ${MASTER} \
        --driver-cores 2  \
        --driver-memory 14g  \
        --total-executor-cores 2  \
        --executor-cores 2  \
        --executor-memory 14g \
        --conf spark.akka.frameSize=64 \
        --py-files ${ANALYTICS_ZOO_PYZIP},${ZOO_TUTORIALS}/keras/tmp_test.py \
        --properties-file ${ANALYTICS_ZOO_CONF} \
        --jars ${ANALYTICS_ZOO_JAR} \
        --conf spark.driver.extraClassPath=${ANALYTICS_ZOO_JAR} \
        --conf spark.executor.extraClassPath=${ANALYTICS_ZOO_JAR} \
        ${ZOO_TUTORIALS}/keras/tmp_test.py
now=$(date "+%s")
time5=$((now-start))
rm ${ZOO_TUTORIALS}/keras/tmp_test.py
echo "#5 4.4-overfitting-and-underfitting time used:$time5 seconds"

echo "#6 5.1-introduction-to-convnets"
#timer
start=$(date "+%s")
${ZOO_TUTORIALS}/keras/ipynb2py.sh ${ZOO_TUTORIALS}/keras/5.1-introduction-to-convnets
sed "s/nb_epoch=5/nb_epoch=1/g" ${ZOO_TUTORIALS}/keras/5.1-introduction-to-convnets.py >${ZOO_TUTORIALS}/keras/tmp_test.py
sed -i "s/get_ipython()/#/g" ${ZOO_TUTORIALS}/keras/tmp_test.py
${SPARK_HOME}/bin/spark-submit \
        --master ${MASTER} \
        --driver-cores 2  \
        --driver-memory 8g  \
        --total-executor-cores 2  \
        --executor-cores 2  \
        --executor-memory 8g \
        --conf spark.akka.frameSize=64 \
        --py-files ${ANALYTICS_ZOO_PYZIP},${ZOO_TUTORIALS}/keras/tmp_test.py  \
        --properties-file ${ANALYTICS_ZOO_CONF} \
        --jars ${ANALYTICS_ZOO_JAR} \
        --conf spark.driver.extraClassPath=${ANALYTICS_ZOO_JAR} \
        --conf spark.executor.extraClassPath=${ANALYTICS_ZOO_JAR} \
        ${ZOO_TUTORIALS}/keras/tmp_test.py
now=$(date "+%s")
time6=$((now-start))
rm ${ZOO_TUTORIALS}/keras/tmp_test.py
echo "#6 5.1-introduction-to-convnets time used:$time6 seconds"


echo "#7 6.2-understanding-recurrent-neural-networks"
#timer
start=$(date "+%s")
${ZOO_TUTORIALS}/keras/ipynb2py.sh ${ZOO_TUTORIALS}/keras/6.2-understanding-recurrent-neural-networks
sed "s/nb_epoch=10/nb_epoch=2/g" ${ZOO_TUTORIALS}/keras/6.2-understanding-recurrent-neural-networks.py >${ZOO_TUTORIALS}/keras/tmp_test.py
sed -i "s/get_ipython()/#/g" ${ZOO_TUTORIALS}/keras/tmp_test.py
sed -i "s/import matplotlib.pyplot as plt/import matplotlib/g" ${ZOO_TUTORIALS}/keras/tmp_test.py
sed -i '/^import matplotlib$/a\matplotlib.use('\''Agg'\'')' ${ZOO_TUTORIALS}/keras/tmp_test.py
sed -i '/^matplotlib.use('\''Agg'\'')$/a\from matplotlib import pyplot as plt' ${ZOO_TUTORIALS}/keras/tmp_test.py
${SPARK_HOME}/bin/spark-submit \
        --master ${MASTER} \
        --driver-cores 2  \
        --driver-memory 8g  \
        --total-executor-cores 2  \
        --executor-cores 2  \
        --executor-memory 8g \
        --conf spark.akka.frameSize=64 \
        --py-files ${ANALYTICS_ZOO_PYZIP},${ZOO_TUTORIALS}/keras/tmp_test.py \
        --properties-file ${ANALYTICS_ZOO_CONF} \
        --jars ${ANALYTICS_ZOO_JAR} \
        --conf spark.driver.extraClassPath=${ANALYTICS_ZOO_JAR} \
        --conf spark.executor.extraClassPath=${ANALYTICS_ZOO_JAR} \
        ${ZOO_TUTORIALS}/keras/tmp_test.py
now=$(date "+%s")
time7=$((now-start))
rm ${ZOO_TUTORIALS}/keras/tmp_test.py
echo "#7 6.2-understanding-recurrent-neural-networks time used:$time7 seconds"

echo "#1 2.1-a-first-look-at-a-neural-network.ipynb"
echo "#2 3.5-classifying-movie-reviews used:$time2 seconds"
echo "#3 3.6-classifying-newswires time used:$time3 seconds"
echo "#4 3.7-predicting-house-prices time used:$time4 seconds"
echo "#5 4.4-overfitting-and-underfitting time used:$time5 seconds"
echo "#6 5.1-introduction-to-convnets time used:$time6 seconds"
echo "#7 6.2-understanding-recurrent-neural-networks time used:$time7 seconds"

