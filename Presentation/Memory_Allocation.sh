spark-submit \
  --master yarn \
  --deploy-mode cluster \
  --num-executors 17 \
  --executor-cores 5 \
  --executor-memory 18g \
  --driver-memory 8g \
  --conf spark.executor.memoryOverhead=3g \
  # Add your application specific configurations and jar file
  your-application.jar 
  # ETL.py / ETL.sc / ETL.jar
