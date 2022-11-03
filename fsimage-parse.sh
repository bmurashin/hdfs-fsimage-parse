#!/bin/bash

set -euxo pipefail

kinit -kt /etc/security/keytabs/hdfs.headless.keytab hdfs-my_cluster_name;

JAVA_OPTS=""
if [[ -n ${USE_LOG4J_XML-} ]]; then JAVA_OPTS="${JAVA_OPTS} -Dlog4j.configuration=file:./log4j.xml"; fi
if [[ -n ${REMOTE_DEBUG-} ]]; then JAVA_OPTS="${JAVA_OPTS} -agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=*:5005"; fi

state=$(curl -s --fail $(hostname -f):50070/jmx?qry=Hadoop:service=NameNode,name=NameNodeStatus | jq -r ".beans[0].State");
if [[ "$state" == "standby" ]]; then

    rm -f fsimage_*;
    timestamp=$(date +"%s");
    hdfs dfsadmin -fetchImage .;

    rm -rf ./orc/;
    /opt/jdk-11.0.11/bin/java \
        -XX:+UseG1GC \
        -Djava.library.path=./hadoop-3.3.0/lib/native/:./libsnappy-1.1.4/ \
        ${JAVA_OPTS} \
        -cp fsimage-parse-1.0.jar fsimage.parse.OrcMultiThreadWriter $(ls fsimage_* | head -n 1) ./orc/fsimage

    beeline='/usr/bin/beeline -u "jdbc:hive2://zk01:2181,zk02:2181,zk03:2181/;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2" --silent --showHeader=false';

    # get all partitions except last 4
    partitions_to_delete=$($beeline -e "SHOW PARTITIONS monitoring.fsimage" | (grep -oP "parsed=\d+" || true) | head -n -4)

    for partition in $partitions_to_delete; do
        $beeline -e "ALTER TABLE monitoring.fsimage DROP PARTITION($partition);";
    done

    # create new partition
    hdfs dfs -mkdir /apps/hive/warehouse/monitoring.db/fsimage/parsed=$timestamp;
    hdfs dfs -put ./orc/fsimage*.orc /apps/hive/warehouse/monitoring.db/fsimage/parsed=$timestamp;

    #$beeline -e "ALTER TABLE monitoring.fsimage ADD PARTITION (parsed="$timestamp");"
    $beeline -e "MSCK REPAIR TABLE monitoring.fsimage;"


    echo "Done";
else
    echo "Active NameNode, stopping";
fi
