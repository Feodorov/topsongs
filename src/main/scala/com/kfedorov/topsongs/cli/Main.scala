package com.kfedorov.topsongs.cli

import com.kfedorov.topsongs.businesslogic.TopSongs
import com.kfedorov.topsongs.repositories.SparkRepository
import org.apache.spark.sql.SparkSession
import org.rogach.scallop._

class Conf(arguments: Seq[String]) extends ScallopConf(arguments) {
  val inputFile = opt[String](required = true)
  val outputFolder = opt[String](required = true)
  verify()
}

object Main {
  def main(args: Array[String]): Unit = {
    val conf = new Conf(args)
    val spark =
      SparkSession.builder.appName("top songs").master("local[*]").getOrCreate()
    val repository = SparkRepository(spark)

    val dataset = repository.read(conf.inputFile())
    val result = TopSongs.topSongs(dataset)

    repository.write(
      conf.outputFolder(),
      result
    )

    spark.stop()
  }
}
