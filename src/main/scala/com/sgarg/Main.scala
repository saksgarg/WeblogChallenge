package com.sgarg

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{StructType, StructField, StringType,
                                        IntegerType, TimestampType, DoubleType, LongType}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

object Main {

  val APPNAME = "Web Log Challenge"
  val LOGLEVEL = "Error"
  val TIMESTAMP = "timestamp"
  val ELB = "elb"
  val REQUEST_IP_PORT = "request_ip_port"
  val BACKEND_IP_PORT = "backend_ip_port"
  val REQUEST_PROCESSING_TIME = "request_processing_time"
  val BACKEND_PROCESSING_TIME = "backend_processing_time"
  val RESPONSE_PROCESSING_TIME = "response_processing_time"
  val ELB_STATUS_CODE = "elb_status_code"
  val BACKEND_STATUS_CODE = "backend_status_code"
  val RECEIVED_BYTES = "received_bytes"
  val SENT_BYTES = "sent_bytes"
  val REQUEST = "request"
  val USER_AGENT = "user_agent"
  val SSL_CIPHER = "ssl_cipher"
  val SSL_PROTOCOL = "ssl_protocol"
  val REQUEST_IP = "request_ip"
  val REQUEST_PORT = "request_port"
  val BACKEND_IP = "backend_ip"
  val BACKEND_PORT = "backend_port"
  val INTEGER = "integer"
  val PROTOCOL = "protocol"
  val REQUEST_VERB = "request_verb"
  val URL = "url"
  val SESSION_ID = "sessionId"
  val SESSION_LENGTH = "session_length"
  val AVG_SESSION_LENGTH = "avg_session_length"
  val UNIQUE_URL_COUNT = "unique_url_count"
  val AVG_UNIQUE_URL_COUNT = "avg_unique_url_count"
  val LONGEST_SESSION_TIME = "longest_session_time"
  val ELB_LOGS_SESSIONIZED_OUT = "data/elb_logs_sessionized_out"
  val AVG_SESSION_LENGTH_OUT = "data/avg_session_length_out"
  val UNIQUE_URL_COUNTS_BY_SESSION_ID_OUT = "data/unique_url_counts_by_session_id_out"
  val MOST_ENGAGED_USERS_OUT = "data/most_engaged_users_out"

  val customSchema = StructType(Array(
    StructField(TIMESTAMP, TimestampType, true),
    StructField(ELB, StringType, true),
    StructField(REQUEST_IP_PORT, StringType, true),
    StructField(BACKEND_IP_PORT, StringType, true),
    StructField(REQUEST_PROCESSING_TIME, DoubleType, true),
    StructField(BACKEND_PROCESSING_TIME, DoubleType, true),
    StructField(RESPONSE_PROCESSING_TIME, DoubleType, true),
    StructField(ELB_STATUS_CODE, StringType, true),
    StructField(BACKEND_STATUS_CODE, StringType, true),
    StructField(RECEIVED_BYTES, LongType, true),
    StructField(SENT_BYTES, LongType, true),
    StructField(REQUEST, StringType, true),
    StructField(USER_AGENT, StringType, true),
    StructField(SSL_CIPHER, StringType, true),
    StructField(SSL_PROTOCOL, StringType, true)))

  val byVisitorOrderedByTimestamp = Window.partitionBy(REQUEST_IP, USER_AGENT).orderBy(TIMESTAMP)

  val quantile_0_95 = Array(0.95)

  def main(args: Array[String]): Unit = {

    if(args.length == 2) {

      val spark = SparkSession.builder.appName(APPNAME).master("local[*]").getOrCreate()
      spark.sparkContext.setLogLevel(LOGLEVEL)

      val path = args(0)
      val session_max_idle_time = args(1).toLong

      var elb_logs = spark.read
        .option("header", "false")
        .schema(customSchema)
        .option("delimiter", " ")
        .option("mode", "DROPMALFORMED")
        .csv(path)

      elb_logs = elb_logs.select(unix_timestamp(col(TIMESTAMP)).alias(TIMESTAMP),
        col(ELB),
        regexp_extract(col(REQUEST_IP_PORT), "^([^ ]*):", 1).alias(REQUEST_IP),
        regexp_extract(col(REQUEST_IP_PORT), ":([0-9]*)$", 1).cast(INTEGER).alias(REQUEST_PORT),
        regexp_extract(col(BACKEND_IP_PORT), "^([^ ]*):", 1).alias(BACKEND_IP),
        regexp_extract(col(BACKEND_IP_PORT), ":([0-9]*)$", 1).cast(INTEGER).alias(BACKEND_PORT),
        col(REQUEST_PROCESSING_TIME),
        col(BACKEND_PROCESSING_TIME),
        col(RESPONSE_PROCESSING_TIME),
        col(ELB_STATUS_CODE),
        col(BACKEND_STATUS_CODE),
        col(RECEIVED_BYTES),
        col(SENT_BYTES),
        regexp_extract(col(REQUEST), "^[\"]?([^ ]*)", 1).alias(REQUEST_VERB),
        regexp_extract(col(REQUEST), "^[^ ]* ([^ ]*)", 1).alias(URL),
        regexp_extract(col(REQUEST), "^[^ ]* [^ ]* ([^ ]*)$", 1).alias(PROTOCOL),
        col(USER_AGENT),
        col(SSL_CIPHER),
        col(SSL_PROTOCOL))

      elb_logs = elb_logs.filter(!(col(TIMESTAMP).isNull) && !(col(REQUEST_IP).isNull) && !(col(USER_AGENT).isNull))

      /*
        GOAL 1: SESSIONIZE THE WEB LOG BY IP.
       */

      var elb_logs_sessionized = elb_logs.withColumn(SESSION_ID,
        concat_ws(
          "_",
          col(REQUEST_IP),
          col(USER_AGENT),
          sum(
            when(((col(TIMESTAMP) - lag(col(TIMESTAMP), 1, 0)
              .over(byVisitorOrderedByTimestamp)) > session_max_idle_time), 1)
              .otherwise(0)
          ).over(byVisitorOrderedByTimestamp)))

      elb_logs_sessionized.write.parquet(ELB_LOGS_SESSIONIZED_OUT)
      elb_logs_sessionized = elb_logs_sessionized.cache()

      /*
        GOAL 2: DETERMINE THE AVERAGE SESSION TIME.
       */

      val session_times = elb_logs_sessionized
        .groupBy(REQUEST_IP, USER_AGENT, SESSION_ID)
        .agg((max(TIMESTAMP) - min(TIMESTAMP)).alias(SESSION_LENGTH))
        .filter(col(SESSION_LENGTH) =!= 0)

      val avg_session_length = session_times.groupBy().agg(round(avg(SESSION_LENGTH), 2).alias(AVG_SESSION_LENGTH))
      println("*"*25 + "AVERAGE SESSION LENGTH (IN SECONDS):" + "*"*25)
      avg_session_length.show(false)
      avg_session_length.write.parquet(AVG_SESSION_LENGTH_OUT)

      /*
        GOAL 3: DETERMINE UNIQUE URL VISITS PER SESSION.
       */

      val unique_url_count = elb_logs_sessionized.groupBy(REQUEST_IP, USER_AGENT, SESSION_ID)
                                                  .agg(countDistinct(URL).alias(UNIQUE_URL_COUNT))
                                                    .sort(desc(UNIQUE_URL_COUNT))
      unique_url_count.write.parquet(UNIQUE_URL_COUNTS_BY_SESSION_ID_OUT)
      println("*"*25 + "UNIQUE URL COUNTS BY SESSION ID:" + "*"*25)
      unique_url_count.show()

      val avg_unique_url_count = unique_url_count.groupBy()
                                                  .agg(round(avg(UNIQUE_URL_COUNT), 2)
                                                    .alias(AVG_UNIQUE_URL_COUNT))
      println("*"*25 + "AVERAGE UNIQUE URL COUNT BY SESSION:" + "*"*25)
      avg_unique_url_count.show(false)

      /*
        GOAL 4: FIND THE MOST ENGAGED USERS.
       */

      val quantile_0_95_session_length = (session_times.stat.approxQuantile(SESSION_LENGTH, quantile_0_95, 0))(0)

      val longest_sessions = session_times.filter(session_times(SESSION_LENGTH) > quantile_0_95_session_length)

      val most_engaged_users = longest_sessions.groupBy(REQUEST_IP, USER_AGENT)
                                                .agg(max(SESSION_LENGTH).alias(LONGEST_SESSION_TIME))
                                                .sort(desc(LONGEST_SESSION_TIME))

      println("*"*25 + "MOST ENGAGED USERS:" + "*"*25)
      most_engaged_users.show(false)
      most_engaged_users.write.parquet(MOST_ENGAGED_USERS_OUT)

      spark.stop()

    } else {
        throw new IllegalArgumentException("illegal argument exception")
    }
  }
}