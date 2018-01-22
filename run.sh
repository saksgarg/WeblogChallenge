#!/bin/bash
# Run script

echo "Web Log Challenge"

sbt clean assembly
gunzip data/2015_07_22_mktplace_shop_web_log_sample.log.gz
$SPARK_HOME/bin/spark-submit target/scala-2.11/WebLogChallenge.jar data/2015_07_22_mktplace_shop_web_log_sample.log 900
gzip data/2015_07_22_mktplace_shop_web_log_sample.log
