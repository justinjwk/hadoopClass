export HADOOP_PREFIX=/usr/local/hadoop/hadoop
export CLASSPATH=$HADOOP_PREFIX/etc/hadoop
BASE_HADOOP_JAR_DIR=$HADOOP_PREFIX/share/hadoop
for f in \
   $BASE_HADOOP_JAR_DIR/common/*.jar   \
   $BASE_HADOOP_JAR_DIR/common/lib/*.jar  \
   $BASE_HADOOP_JAR_DIR/hdfs/*.jar \
   $BASE_HADOOP_JAR_DIR/hdfs/lib/*.jar \
   $BASE_HADOOP_JAR_DIR/yarn/*.jar \
   $BASE_HADOOP_JAR_DIR/yarn/lib/*.jar \
   $BASE_HADOOP_JAR_DIR/mapreduce/*.jar \
   $BASE_HADOOP_JAR_DIR/mapreduce/lib/*.jar
do
  CLASSPATH=$CLASSPATH:$f
done
JAR=`find out -name *.jar`
java -classpath ${CLASSPATH}:$JAR bdpuh.hw2.ParallelLocalToHdfsCopy1 /home/hdadmin/programming/test /test 3

