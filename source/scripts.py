# Setup Hadoop and Spark
SQL_script = '''#!/bin/bash
sudo apt-get update && sudo apt-get upgrade -y
sudo apt-get install -y python3 python3-pip default-jdk wget scala git 

cd /home/ubuntu
wget https://dlcdn.apache.org/hadoop/common/hadoop-3.4.0/hadoop-3.4.0.tar.gz
wget https://downloads.apache.org/hadoop/common/hadoop-3.4.0/hadoop-3.4.0.tar.gz.sha512

tar -xzvf hadoop-3.4.0.tar.gz
sudo mv hadoop-3.4.0 /usr/local/hadoop

# Configuring Hadoop Java Home
export HADOOP_HOME=/usr/local/hadoop
export PATH=$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin
export JAVA_HOME=$(readlink -f /usr/bin/java | sed "s:/bin/java::")
export PATH=$PATH:$JAVA_HOME/bin

# Installing Spark
wget https://dlcdn.apache.org/spark/spark-3.5.3/spark-3.5.3-bin-hadoop3.tgz
wget https://downloads.apache.org/spark/spark-3.5.3/spark-3.5.3-bin-hadoop3.tgz.sha512
tar -xzvf spark-*.tgz
sudo mv spark-3.5.3-bin-hadoop3 /usr/local/spark
export SPARK_HOME=/usr/local/spark
export PATH=$PATH:$SPARK_HOME/bin

# Running wordcount for pg4300.txt on hadoop
wget https://www.gutenberg.org/cache/epub/4300/pg4300.txt
{ time /usr/local/hadoop/bin/hadoop jar /usr/local/hadoop/share/hadoop/mapreduce/hadoop-mapreduce-examples-3.4.0.jar wordcount /home/ubuntu/pg4300.txt /home/ubuntu/output > /dev/null; } 2> hadoop_time_exploration.txt
cat ./output/part-r-00000 > output.txt # ou hadoop fs -cat /output/part-r-00000 > output_4_3_hadoop.txt

# Running wordcount for pg4300.txt on local
{ time cat pg4300.txt | tr ' ' '\n' | sort | uniq -c > output_4_3_linux.txt; } 2> ubuntu_time_exploration.txt

# Running wordcount for pg4300.txt on spark
spark-submit /usr/local/spark/examples/src/main/python/wordcount.py pg4300.txt > output_4_3_spark.txt

# 4.4 Who wins ?
# mkdir input_4_4

wget https://www.gutenberg.ca/ebooks/buchanj-midwinter/buchanj-midwinter-00-t.txt # buchanj-midwinter-00-t.txt
wget https://www.gutenberg.ca/ebooks/carman-farhorizons/carman-farhorizons-00-t.txt # carman-farhorizons-00-t.txt
wget https://www.gutenberg.ca/ebooks/colby-champlain/colby-champlain-00-t.txt # colby-champlain-00-t.txt
wget https://www.gutenberg.ca/ebooks/cheyneyp-darkbahama/cheyneyp-darkbahama-00-t.txt # cheyneyp-darkbahama-00-t.txt
wget https://www.gutenberg.ca/ebooks/delamare-bumps/delamare-bumps-00-t.txt # delamare-bumps-00-t.txt
wget https://www.gutenberg.ca/ebooks/charlesworth-scene/charlesworth-scene-00-t.txt # charlesworth-scene-00-t.txt
wget https://www.gutenberg.ca/ebooks/delamare-lucy/delamare-lucy-00-t.txt # delamare-lucy-00-t.txt
wget https://www.gutenberg.ca/ebooks/delamare-myfanwy/delamare-myfanwy-00-t.txt  # delamare-myfanwy-00-t.txt
wget https://www.gutenberg.ca/ebooks/delamare-penny/delamare-penny-00-t.txt # delamare-penny-00-t.txt

files=(
    "/home/ubuntu/buchanj-midwinter-00-t.txt"
    "/home/ubuntu/carman-farhorizons-00-t.txt"
    "/home/ubuntu/colby-champlain-00-t.txt"
    "/home/ubuntu/cheyneyp-darkbahama-00-t.txt"
    "/home/ubuntu/delamare-bumps-00-t.txt"
    "/home/ubuntu/charlesworth-scene-00-t.txt"
    "/home/ubuntu/delamare-lucy-00-t.txt"
    "/home/ubuntu/delamare-myfanwy-00-t.txt"
    "/home/ubuntu/delamare-penny-00-t.txt"
)

hadoop_times_file="output_hadoop_times.txt"
hadoop_output_dir="/home/ubuntu/output"
hadoop_cmd="/usr/local/hadoop/bin/hadoop jar /usr/local/hadoop/share/hadoop/mapreduce/hadoop-mapreduce-examples-3.4.0.jar wordcount"

spark_times_file="output_spark_times.txt"
spark_cmd="spark-submit /usr/local/spark/examples/src/main/python/wordcount.py"

touch "$hadoop_times_file"

for file in "${files[@]}"; do
    for run in {1..3}; do
        if [ -d "$hadoop_output_dir" ]; then
            rm -r "$hadoop_output_dir"
        fi
        { time $hadoop_cmd "$file" "$hadoop_output_dir" > /dev/null; } 2>> "$hadoop_times_file"
        { time $spark_cmd "$file" > /dev/null; } 2>> "$spark_times_file"
    done
done
'''

# 4.4 Who wins ?
# Download the following text files from the Gutenberg project
benchmark_datasets= '''#!/bin/bash
wget https://www.gutenberg.ca/ebooks/buchanj-midwinter/buchanj-midwinter-00-t.txt # buchanj-midwinter-00-t.txt
wget https://www.gutenberg.ca/ebooks/carman-farhorizons/carman-farhorizons-00-t.txt # carman-farhorizons-00-t.txt
wget https://www.gutenberg.ca/ebooks/colby-champlain/colby-champlain-00-t.txt # colby-champlain-00-t.txt
wget https://www.gutenberg.ca/ebooks/cheyneyp-darkbahama/cheyneyp-darkbahama-00-t.txt # cheyneyp-darkbahama-00-t.txt
wget https://www.gutenberg.ca/ebooks/delamare-bumps/delamare-bumps-00-t.txt # delamare-bumps-00-t.txt
wget https://www.gutenberg.ca/ebooks/charlesworth-scene/charlesworth-scene-00-t.txt # charlesworth-scene-00-t.txt
wget https://www.gutenberg.ca/ebooks/delamare-lucy/delamare-lucy-00-t.txt # delamare-lucy-00-t.txt
wget https://www.gutenberg.ca/ebooks/delamare-myfanwy/delamare-myfanwy-00-t.txt  # delamare-myfanwy-00-t.txt
wget https://www.gutenberg.ca/ebooks/delamare-penny/delamare-penny-00-t.txt # delamare-penny-00-t.txt

files=(
    "/home/ubuntu/buchanj-midwinter-00-t.txt"
    "/home/ubuntu/carman-farhorizons-00-t.txt"
    "/home/ubuntu/colby-champlain-00-t.txt"
    "/home/ubuntu/cheyneyp-darkbahama-00-t.txt"
    "/home/ubuntu/delamare-bumps-00-t.txt"
    "/home/ubuntu/charlesworth-scene-00-t.txt"
    "/home/ubuntu/delamare-lucy-00-t.txt"
    "/home/ubuntu/delamare-myfanwy-00-t.txt"
    "/home/ubuntu/delamare-penny-00-t.txt"
)

hadoop_times_file="output_hadoop_times.txt"
hadoop_output_dir="/home/ubuntu/output"
hadoop_cmd="/usr/local/hadoop/bin/hadoop jar /usr/local/hadoop/share/hadoop/mapreduce/hadoop-mapreduce-examples-3.4.0.jar wordcount"

spark_times_file="output_spark_times.txt"
spark_cmd="spark-submit /usr/local/spark/examples/src/main/python/wordcount.py"

touch "$hadoop_times_file"

for file in "${files[@]}"; do
    for run in {1..3}; do
        if [ -d "$hadoop_output_dir" ]; then
            rm -r "$hadoop_output_dir"
        fi
        { time $hadoop_cmd "$file" "$hadoop_output_dir" > /dev/null; } 2>> "$hadoop_times_file"
        { time $spark_cmd "$file" > /dev/null; } 2>> "$spark_times_file"
    done
done
'''
