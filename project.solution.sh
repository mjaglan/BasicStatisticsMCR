myPWD=$(pwd)
pName="BasicStatisticsMCR"
HDFS_HOME_PATH=/user/summer

# clear previous logs
rm -rf $HADOOP_HOME/logs/userlogs/*

# build project
./project.build.sh $pName

echo "remove all folders"
hadoop dfs -rmr $HDFS_HOME_PATH/*

echo "Create directory in HDFS for input data"
hadoop dfs -mkdir $HDFS_HOME_PATH/input

echo "input data: Copy input data to HDFS"
hadoop dfs -put ./input/input_BasicStatistics.txt $HDFS_HOME_PATH/input/

echo "Check results"
sleep 5s
hadoop dfs -ls $HDFS_HOME_PATH/input

echo "================ RUN HADOOP JAR =================="
sleep 5s
hadoop jar ./build/libs/$pName.jar edu.indiana.soic.cs.$pName input output

echo "Check results"
sleep 5s
hadoop dfs -ls $HDFS_HOME_PATH

# make output dir
mkdir -p output

#echo "Compare results"
rm -rf ./output/*
hadoop dfs -copyToLocal $HDFS_HOME_PATH/* ./output/

cd $myPWD
