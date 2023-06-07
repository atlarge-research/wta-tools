#!/bin/bash
export SPARK_HOME=$SPARK_HOME
export MASTER=local[4]
export ANALYTICS_ZOO_HOME=$ANALYTICS_ZOO_HOME
export ANALYTICS_ZOO_JAR=`find ${ANALYTICS_ZOO_HOME}/lib -type f -name "analytics-zoo*jar-with-dependencies.jar"`
export ANALYTICS_ZOO_PYZIP=`find ${ANALYTICS_ZOO_HOME}/lib -type f -name "analytics-zoo*python-api.zip"`
export ANALYTICS_ZOO_CONF=${ANALYTICS_ZOO_HOME}/conf/spark-analytics-zoo.conf
export PYTHONPATH=${ANALYTICS_ZOO_PYZIP}:$PYTHONPATH

chmod +x ${ZOO_TUTORIALS}/orca/tf2/ipynb2py.sh

set -e

testdir="${ZOO_TUTORIALS}/orca/tf2"
timer=()
FILES=${testdir}/*.ipynb
index=1
for f in ${FILES}
do
	filename="${f##*/}"
	notebookname="${filename%.*}"
	echo "#${index} ${notebookname}"
	#timer
	start=$(date "+%s")
	${ZOO_TUTORIALS}/orca/tf2/ipynb2py.sh ${testdir}/${notebookname}
	cat ${testdir}/${notebookname}.py > ${testdir}/tmp_test.py
	# Specific notebook adjustments
	if [[ "$notebookname" == *"6.2"* ]]; then
		sed "s/max_epoch = 10/max_epoch = 2/g" ${testdir}/${notebookname}.py > ${testdir}/tmp_test.py
	fi
	if [[ "$notebookname" == *"3.7"* ]]; then
		sed "s/num_epochs = 500/num_epochs = 10/g" ${testdir}/${notebookname}.py > ${testdir}/tmp_test.py
	fi
	if [[ "$notebookname" == *"8.1"* ]]; then
		sed "s/for epoch in range(1, 60)/for epoch in range(1, 2)/g" ${testdir}/${notebookname}.py > ${testdir}/tmp_test.py
	fi
	sed -i "s/plt.show()/#/g" ${testdir}/tmp_test.py    # showing the plot may stuck the test

	${SPARK_HOME}/bin/spark-submit \
        	--master ${MASTER} \
        	--driver-cores 2  \
        	--driver-memory 12g  \
        	--total-executor-cores 2  \
        	--executor-cores 2  \
        	--executor-memory 12g \
        	--conf spark.akka.frameSize=64 \
        	--py-files ${ANALYTICS_ZOO_PYZIP},${testdir}/tmp_test.py \
        	--properties-file ${ANALYTICS_ZOO_CONF} \
        	--jars ${ANALYTICS_ZOO_JAR} \
        	--conf spark.driver.extraClassPath=${ANALYTICS_ZOO_JAR} \
        	--conf spark.executor.extraClassPath=${ANALYTICS_ZOO_JAR} \
        	${testdir}/tmp_test.py
	now=$(date "+%s")
	timer+=($((now-start)))
	index=$((index+1))
	rm ${testdir}/${notebookname}.py
done

rm ${testdir}/tmp_test.py

# Print summary
echo "Summary:"
index=1
for f in $FILES
do
	filename="${f##*/}"
        notebookname="${filename%.*}"
	echo "#${index} ${notebookname} used: ${timer[$((index-1))]} seconds"
	index=$((index+1))
done

