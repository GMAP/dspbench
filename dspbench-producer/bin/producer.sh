#!/bin/bash

usage() {
    echo "Usage: $0 COMMAND"
    echo ""
    echo "Commands:"
    echo "  start        <properties-file> <topic> <partitions> <replicas> <data-dir>"
    echo "  stop         <properties-file> <topic>"
    echo "  start-socket <properties-file> <data-dir>"
    echo "  stop-socket  <properties-file>"
    exit 1
}
[ "$#" -ge 2 ] || usage

BASE_DIR=$(dirname $0)
COMMAND=$1
JAR_FILE=$BASE_DIR/streamer-producer.jar
PID_FILE=$BASE_DIR/$3.pid
LOG_FILE=$BASE_DIR/$3.log

if [ "$COMMAND" == "start" ]; then
    echo -ne "Starting producer... "
    echo -ne "" > $PID_FILE
 
    nohup java -jar $JAR_FILE start $2 $3 $4 $5 $6 > $LOG_FILE 2>&1 &
    echo $! >> $PID_FILE

    echo -ne "DONE\n"
    exit 0
elif [ "$COMMAND" == "create_topic" ]; then
    echo -ne "Creating Topic... "
    echo -ne "" > $PID_FILE
 
    nohup java -jar $JAR_FILE start $2 $3 $4 $5 $6 > $LOG_FILE 2>&1 &
    echo $! >> $PID_FILE

    echo -ne "DONE\n"
    exit 0
elif [ "$COMMAND" == "stop" ]; then
    echo -ne "Stopping producer... "
    
    while read pid; do
        kill -9 $pid
    done <$PID_FILE

    #echo -ne "Removing topic..."
    #java -jar $JAR_FILE stop $2 $3 >> $LOG_FILE 2>&1
    
    echo -ne "" > $PID_FILE
    echo -ne "DONE\n"

elif [ "$COMMAND" == "start-socket" ]; then
    echo -ne "Starting socket producer... "
    echo -ne "" > $PID_FILE
 
    nohup java -cp $JAR_FILE com.streamer.producer.SocketProducer start $2 $3 > $LOG_FILE 2>&1 &
    echo $! >> $PID_FILE

    echo -ne "DONE\n"
    exit 0

elif [ "$COMMAND" == "stop-socket" ]; then
    echo -ne "Stopping socket producer... "
    
    while read pid; do
        kill -9 $pid
    done <$PID_FILE

    java -cp $JAR_FILE com.streamer.producer.SocketProducer stop $2 >> $LOG_FILE 2>&1
    
    echo -ne "" > $PID_FILE
    echo -ne "DONE\n"

else
    usage
fi