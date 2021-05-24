
CARBON_CLASSPATH=""
CARBON_HOME=$PWD
echo "The current working directory: $CARBON_HOME"

for f in $CARBON_HOME/lib/*.jar
do
	if [ "$f" != "$CARBON_HOME/lib/*.jar" ];then
        CARBON_CLASSPATH="$CARBON_CLASSPATH":$f
    fi
done
CARBON_CLASSPATH="$CARBON_CLASSPATH":$CARBON_HOME/conf/log4j.properties
java -classpath "$CARBON_HOME/plugins/org.wso2.carbon.apimgt.analytics.thrift.client-1.0.0.jar:$CARBON_CLASSPATH" org.wso2.carbon.apimgt.thrift.Client