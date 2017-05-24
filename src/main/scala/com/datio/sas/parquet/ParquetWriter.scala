package com.datio.sas.parquet


import org.apache.spark.sql.{DataFrame, DataFrameWriter, SaveMode}
import com.typesafe.config.Config

import scala.collection.JavaConverters._

/**
  * Created by dvega on 19/05/17.
  */
class ParquetWriter(implicit config: Config)  {

  /**
    * Write a dataframe in Parquet.
    *
    * @param df Dataframe you want to write.
    */
  def write(df: DataFrame): Unit = {
    val path = config.getString("path")
    val dataWriter = df.coalesce(1).write
    val savemode = Option(config.getString("savemode"))
      .getOrElse("ErrorIfExists").toLowerCase() match {
      case "append" => SaveMode.Append
      case "overwrite" => SaveMode.Overwrite
      case "errorifexists" | _ => SaveMode.ErrorIfExists
    }
    setOptions(dataWriter).mode(savemode).parquet(path)
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
