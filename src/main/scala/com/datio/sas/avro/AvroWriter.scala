package com.datio.sas.avro

import com.databricks.spark.avro._
import com.datio.sas.avro.AvroWriter._
import com.typesafe.config.Config
import org.apache.spark.sql._

import scala.collection.JavaConverters._

/**
  * Class to write in Avro using spark-avro library and its options.
  * Takes an implicit parameter which is a Config following this format.
  * {
  * path = "/path/to/target/"
  * options {
  *   compression = "deflate"
  *   deflate.level = "5"
  *   ...
  * }
  * }
  * To see othe options check out https://github.com/databricks/spark-avro
  */
class AvroWriter(sqc: SQLContext)(implicit config: Config) {

  /**
    * Write a dataframe in Avro.
    *
    * @param df Dataframe you want to write.
    */
  def write(df: Dataset<Row>): Unit = {
    val compression = config.getString(OPTION_COMPRESSION_CODEC)
    sqc.setConf(COMPRESSION_CODEC, compression)
    if (compression.equalsIgnoreCase("deflate")) {
      sqc.setConf(DEFLATE_COMPRESSION_LEVEL, config.getString(OPTION_DEFLATE_LEVEL))
    }

    val savemode = Option(config.getString(SAVEMODE))
      .getOrElse("ErrorIfExists").toLowerCase() match {
      case "append" => SaveMode.Append
      case "overwrite" => SaveMode.Overwrite
      case "errorifexists" | _ => SaveMode.ErrorIfExists
    }

    val path = config.getString("path")
    setOptions(df).mode(savemode).avro(path)
  }

  /**
    * Apply options from configuration to the DataFrame writer.
    *
    * @param dfw
    * @return
    */
  protected def setOptions(dfw: Dataset): DataFrameWriter = {
    val optionsConfig = config.getConfig(OPTIONS_SUBCONFIG)

    // DataFrameWriter After partitioning
    val dfwAP = if (optionsConfig.hasPath(PARTITION_BY)) {
      val partitionBySeq = config.getList(PARTITION_BY).unwrapped().asScala.map(_.toString).toList
      dfw.partitionBy(partitionBySeq: _*)
    } else {
      dfw
    }

    // DataFrameWriter after record names
    if (optionsConfig.hasPath(RECORD_NAME) && optionsConfig.hasPath(RECORD_NAMESPACE)) {
      val parameters = Map(RECORD_NAME -> optionsConfig.getString(RECORD_NAME),
        RECORD_NAMESPACE -> optionsConfig.getString(RECORD_NAMESPACE))
      dfwAP.options(parameters)
    } else {
      dfwAP
    }

  }
}

object AvroWriter {
  val RECORD_NAMESPACE = "recordNamespace"
  val RECORD_NAME = "recordName"
  val PARTITION_BY = "partitionBy"
  val SAVEMODE = "savemode"

  val COMPRESSION_CODEC = "spark.sql.avro.compression.codec"
  val DEFLATE_COMPRESSION_LEVEL = "spark.sql.avro.deflate.level"

  val OPTIONS_SUBCONFIG = "options"
  val OPTION_COMPRESSION_CODEC = "options.compression"
  val OPTION_DEFLATE_LEVEL = "options.deflate.level"
}