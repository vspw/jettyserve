#!/usr/bin/env bash
HADOOP_HOME_PATH=/usr/hdp/current/hadoop-client
HADOOP_CONFIG_SCRIPT=$HADOOP_HOME_PATH/libexec/hadoop-config.sh
HADOOP_CLIENT_LIBS=$HADOOP_HOME_PATH/client
ATLAS_SQOOP_LIBS=/usr/hdp/current/atlas-client/hook/sqoop/atlas-sqoop-plugin-impl
ATLAS_CONF_DIR=/etc/atlas/conf
if [ -e $HADOOP_CONFIG_SCRIPT ] ; then
        .  $HADOOP_CONFIG_SCRIPT
else
        echo "Hadoop Client not Installed on Node"
        exit 1
fi

WORKING_DIR="/opt/atlas/jettyServAtlas"
LOG4J_PROP_FILE=$WORKING_DIR/log4j.properties
export WORK_PATH=$WORKING_DIR/*
PIDFILE=$WORKING_DIR/pid.tmp
OPTS="-Dlog4j.configuration=file:$LOG4J_PROP_FILE"

##Class and options passed
CLASS=jettyserve.MainClass
ATLAS_SERVER_HOST="zulu.hdp.com"
PORT="3333"

CURR_PROGRAM=jetty-serv-atlas
case "${1:-}" in
 start)
        echo "Checking for previously running jetty-serv-atlas..."
        if [ -f $PIDFILE ]; then
          PID=`cat $PIDFILE`
          if ! (ps -p $PID >/dev/null 2>/dev/null); then
            echo "$PIDFILE found with no process. Removing $PID..."
            rm -f $PIDFILE
          else
            tput bold
            echo "ERROR:$CURR_PROGRAM already running"
            tput sgr0
            echo "Check $PIDFILE for PID."
            exit -1
          fi
        fi

        echo "Starting $CURR_PROGRAM"
        nohup java -Dproc_$CLASS $OPTS -cp "$WORK_PATH:$ATLAS_SQOOP_LIBS/*:$ATLAS_CONF_DIR:$HADOOP_CLIENT_LIBS/*" $CLASS $ATLAS_SERVER_HOST $PORT  /tmp 2>> ./ra_error.log >> /dev/null &
        PID=$!
        echo $PID > $PIDFILE
        sleep 2
        echo "Verifying $CURR_PROGRAM process status..."
        if ! (ps -p $PID >/dev/null 2>/dev/null); then
          echo "ERROR: $CURR_PROGRAM start failed."
          exit -1
        fi
        tput bold
        echo "$CURR_PROGRAM successfully started"
        tput sgr0
        echo "Agent PID at: $PIDFILE"
        ;;

 stop)
        if [ -f $PIDFILE ]; then
          PID=`cat $PIDFILE`
          echo "Found $CURR_PROGRAM PID: $PID"
          if ! (ps -p $PID >/dev/null 2>/dev/null); then
            tput bold
            echo "ERROR: $CURR_PROGRAM not running. Stale PID File at: $PIDFILE"
            tput sgr0
          else
            echo "Stopping $CURR_PROGRAM"
            kill -9 $PID
          fi
          echo "Removing PID file at $PIDFILE"
          rm -f $PIDFILE
          tput bold
          echo "$CURR_PROGRAM successfully stopped"
          tput sgr0
        else
          tput bold
          echo "$CURR_PROGRAM is not running. No PID found at $PIDFILE"
          tput sgr0
        fi
        ;;

 restart)
        echo -e "Restarting $CURR_PROGRAM"
        scriptpath=$0
        $scriptpath stop
        shift
        $scriptpath start "$@"
        retcode=$?
        ;;
  *)
        tput bold
        echo "Usage: $0 {start|stop|restart}"
        tput sgr0
        exit 1
        ;;
esac
