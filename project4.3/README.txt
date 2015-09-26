LGM.java >> Language Generating Model MapReduce job and generating HFiles format for hbase bulk loading
HBaseBulkLoad.java >> loading HFiles to hbase table

Commands:
Language Generating Model MapReduce job:
mkdir lmg_classes
javac -classpath ${HADOOP_HOME}/hadoop-core.jar -d lmg_classes LMG.java
/usr/lib/jvm/*/bin/jar -cvf lmg.jar -C lmg_classes/ .

hadoop distcp s3://tpan55987/pr4_1/ /mnt/dataset/

bin/hadoop jar lmg.jar LMG /mnt/dataset/ /mnt/output 2 5
------
Create table:
hbase org.apache.hadoop.hbase.util.RegionSplitter wp -c 5 -f wordpairs

------
Bulk loading:
mkdir load_class
javac -classpath ${HADOOP_HOME}/hadoop-core.jar:${HADOOP_HOME}/lib/commons-configuration-1.6.jar:${HADOOP_HOME}/lib/commons-lang-2.6.jar:${HADOOP_HOME}/lib/commons-logging-1.1.1.jar:hadoop-core-1.0.3.jar:${HADOOP_HOME}/lib/hbase-0.92.0.jar:${HADOOP_HOME}/lib/slf4j-api-1.7.4.jar:${HADOOP_HOME}/lib/zookeeper-3.4.3.jar -d load_class HBaseBulkLoad.java
/usr/lib/jvm/*/bin/jar -cvf load.jar -C load_class/ .

bin/hadoop jar load.jar HBaseBulkLoad /mnt/output/ /mnt/output2 wp
hbase org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles /mnt/output2 wp

