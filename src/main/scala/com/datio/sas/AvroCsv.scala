package com.datio.sas

import java.io.File

import com.datio.sas.avro.AvroReader
import com.datio.sas.csv.CsvWriter
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.{SparkConf, SparkContext, SparkFiles}
import org.slf4j.LoggerFactory

object AvroCsv {

  def main(args: Array[String]): Unit = {

    val log = LoggerFactory.getLogger(this.getClass)

    val sparkConf = new SparkConf()

    val ss = SparkSession.builder().config(sparkConf).master("local").getOrCreate()

    val sqlc = ss.sqlContext

    val configFile = SparkFiles.get("application.conf")
    log.info(s"Path to application.conf is: $configFile")
    val catConfigFile = scala.io.Source.fromFile(configFile).mkString
    log.info(catConfigFile)

    val config = ConfigFactory.load(ConfigFactory.parseFile(new File(configFile)))

    val csvDF = new AvroReader(sqlc)(config.getConfig("sas.avro")).read
    new CsvWriter(ss)(config.getConfig("sas.csv")).write(csvDF)

    ss.stop()
  }

}
