package com.datio.sas

import java.io.File

import com.datio.sas.csv.CsvWriterProbando
import com.datio.sas.parquet.ParquetReader
import com.typesafe.config.ConfigFactory
import org.apache.spark.{SparkConf, SparkContext, SparkFiles}
import org.apache.spark.sql.SQLContext
import org.slf4j.LoggerFactory

/**
  * Created by davidvegaperez on 24/5/17.
  */
object ParquetCsvProbando {
  def main(args: Array[String]): Unit = {

    val log = LoggerFactory.getLogger(this.getClass)

    val sparkConf = new SparkConf()

    val sc = new SparkContext(sparkConf)
    val sqc = new SQLContext(sc)

    val configFile = SparkFiles.get("application.conf")
    log.info(s"Path to application.conf is: $configFile")
    val catConfigFile = scala.io.Source.fromFile(configFile).mkString
    log.info(catConfigFile)

    val config = ConfigFactory.load(ConfigFactory.parseFile(new File(configFile)))

    val parquetDF = new ParquetReader(sqc)(config.getConfig("sas.parquet")).read
    new CsvWriterProbando(sqc)(config.getConfig("sas.csv")).write(parquetDF)

    sc.stop()
  }
}
