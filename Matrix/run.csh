#!/bin/csh -f
#

~/Software/hadoop/bin/hadoop dfs -rmr matrix
~/Software/hadoop/bin/hadoop dfs -mkdir matrix
~/Software/hadoop/bin/hadoop dfs -put ../data/B_$2.txt matrix/a.txt
rm -rf /tmp/matrix
setenv CLASSPATH "$HADOOP_HOME/hadoop-core-1.1.1.jar"
javac -d Inverse *.java
jar -cvf Inverse.jar -C Inverse/ .
date
set start = `date`
${HADOOP_HOME}/bin/hadoop jar Inverse.jar matrix.Inverse 1000 $1
echo $start
date
