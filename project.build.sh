pName="$1"
classesPath="./bin"

if [ -d $classesPath ]; then
		rm -rf $classesPath
fi
mkdir -p $classesPath

# Compile project
javac -classpath $HADOOP_HOME/hadoop-core-1.1.2.jar:$HADOOP_HOME/lib/commons-cli-1.2.jar -d ./$classesPath src/main/java/edu/indiana/soic/cs/$pName.java

# Create the Jar
rm -rf $pName.jar
sleep 2s
jar -cvf build/libs/$pName.jar -C ./$classesPath/ .
 
