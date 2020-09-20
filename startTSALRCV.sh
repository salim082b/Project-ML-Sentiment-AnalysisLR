#!/bin/bash
SERVICE="startpTSALRCV.sh"
JSERVICE="Elasticsearch"
while true
do

if jps |grep "$JSERVICE" >/dev/null
then
    echo "$JSERVICE is running"
else
    echo "$JSERVICE stopped"
    ~/elasticsearch/bin/elasticsearch &
    sleep 60
fi

if ps -C "$SERVICE" >/dev/null
then
    echo "$SERVICE is running"
else
    echo "$SERVICE stopped"
    ~/08ProjectTSALR/"$SERVICE" 
fi
sleep 60
done

