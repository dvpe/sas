package com.datio.sas.csv

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
class CsvWriter(sqc: SQLContext)(implicit config: Config) {

  /**
    * Write a dataframe in CSV.
    *
    * @param df Dataframe you want to write.
    */
  def write(df: DataFrame): Unit = {
    val path = config.getString("path")
    val dataWriter = df.coalesce(1).write.format("com.databricks.spark.csv")
    val savemode = Option(config.getString("savemode"))
      .getOrElse("ErrorIfExists").toLowerCase() match {
      case "append" => SaveMode.Append
      case "overwrite" => SaveMode.Overwrite
      case "errorifexists" | _ => SaveMode.ErrorIfExists
    }
    setOptions(dataWriter).mode(savemode).save(path)
  }

  /**
    * Apply options from configuration to the DataFrame writer.
    *
    * @param dfw
    * @return
    */
  protected def setOptions(dfw: DataFrameWriter): DataFrameWriter = {
    val optionsConfig = config.getConfig("options")
    val entries = optionsConfig.entrySet().asScala.toList

    entries.map(_.getKey).foldLeft(dfw)((dfr, key) => dfr.option(key, optionsConfig.getString(key)))
  }
}
