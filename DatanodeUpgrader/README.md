Offline Datanode layout upgrader

The existing datanodes should be running with HDFS-7928, otherwise this tool will do nothing.

Gracefully shutdown datanode using "dfsadmin -shutdownDatanode" for the replica list files to be generated.

It assumes the volumes are mounted at /grid/n/ where n=0,1,2,3...

Normally run:
sudo -u hdfs hadoop jar /whereartthou/DatanodeUpgrader-0.99.jar net.fayoly.hadoop.DatanodeUpgrader
