sh /application/search/spark-2.1.0-hadoop2.7/bin/spark-submit \
--master spark://10.10.160.151:7077 \
--driver-memory 2g \
--executor-cores 4 \
--total-executor-cores 28 \
--executor-memory 2g \
--jars /application/search/emergency_detector/data/spark-streaming-kafka-0-8-assembly_2.11-2.1.0.jar \
--py-files /application/search/emergency_detectordb_utils.py \
/application/search/emergency_detector/streaming_app.py