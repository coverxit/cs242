hdfs dfs -rm -r -f /user/rwu034/index/input/*
hdfs dfs -rm -r -f /user/rwu034/index/output
HADOOP_CLIENT_OPTS="-Xmx16g" hadoop jar /extra/rwu034/cs242/cs242.jar mapreduce /user/rwu034/index/input /user/rwu034/index/output > output.log 2>&1
hdfs dfs -ls /user/rwu034/index/output/
hdfs dfs -tail /user/rwu034/index/output/part-r-00000
hdfs dfs -get /user/rwu034/index/output/part-r-00000 index.json
HADOOP_CLIENT_OPTS="-Xmx16g" hadoop jar cs242.jar mapreduce /index/input /index/output > output.log 2>&1
