source spark.env

echo "SPARK_HOME:${SPARK_HOME}"

echo "args:$@"

nohup java -Dserver.port=19002 -Dconfig.path="/home/test/riskwarning/0524/app.properties" -jar riskwarning.msg-0.0.1.jar > spark-submit.out 2>&1 &