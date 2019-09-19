package com.example.BostonCrimesMap

import org.apache.spark.{SparkConf}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.broadcast

/*
sbt new holdenk/sparkProjectTemplate.g8
sbt assembly
(base) feynman@Feynman-VirtualBox:~/Spark/spark-2.4.4-bin-hadoop2.7/bin$ sudo ./spark-submit --master local[*] --class com.example.BostonCrimesMap.LocalApp "/home/feynman/bostoncrimesmap/BostonCrimesMap.jar" "/home/feynman/spark_demo/crime.csv" "/home/feynman/spark_demo/offense_codes.csv" "/home/feynman/spark_demo/statWithMediansAndFreq_parquet2/"


https://spark.apache.org/docs/latest/submitting-applications.html
*/

/**
 * Use this to test the app locally, from sbt
 */
object BostonCrimesMap extends App{
  val (crimeFile, offenceCodesFile, outputFolder) = (args(0), args(1), args(2))
  /*
  val (crimeFile, offenceCodesFile, outputFolder) = (
    "/home/feynman/spark_demo/crime.csv",
    "/home/feynman/spark_demo/offense_codes.csv",
    "/home/feynman/spark_demo/statWithMediansAndFreq_parquet/")
  */

  val conf = new SparkConf()
    .setMaster("local")
    .setAppName("My Otus dz w17")

  Runner.run(conf, crimeFile, offenceCodesFile, outputFolder)
}

/**
  * Use this to test the app locally, from sbt
  */
object LocalApp extends App{
  val (crimeFile, offenceCodesFile, outputFolder) = (args(0), args(1), args(2))
  /*
  val (crimeFile, offenceCodesFile, outputFolder) = (
    "/home/feynman/spark_demo/crime.csv",
    "/home/feynman/spark_demo/offense_codes.csv",
    "/home/feynman/spark_demo/statWithMediansAndFreq_parquet/")
  */

  val conf = new SparkConf()
    .setMaster("local")
    .setAppName("My Otus dz w17")

  Runner.run(conf, crimeFile, offenceCodesFile, outputFolder)
}

/**
  * Use this when submitting the app to a cluster with spark-submit
  * */
object BostonCrimesMapApp extends App{
  val (crimeFile, offenceCodesFile, outputFolder) = (args(0), args(1), args(2))
  /*
  val (crimeFile, offenceCodesFile, outputFolder) = (
    "/home/feynman/spark_demo/crime.csv",
    "/home/feynman/spark_demo/offense_codes.csv",
    "/home/feynman/spark_demo/statWithMediansAndFreq_parquet/")
   */

  // spark-submit command should supply all necessary config elements
  Runner.run(new SparkConf(), crimeFile, offenceCodesFile, outputFolder)
}

object Runner {
  def run(conf: SparkConf,
          crimeFile: String = "/home/feynman/spark_demo/crime.csv",
          offenceCodesFile: String = "/home/feynman/spark_demo/offense_codes.csv",
          outputFolder: String = "/home/feynman/spark_demo/statWithMediansAndFreq_parquet/"
         ): Unit = {
    val spark = {
      SparkSession.builder()
        .config(conf)
        .master("local[*]")
        .getOrCreate()
    }

    val crimeFacts = spark
      .read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(crimeFile)

    val offenseCodes = spark
      .read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(offenceCodesFile)

    val offenseCodesBroadcast = broadcast(offenseCodes)

    val statWithMediansAndFreq = BostonCrimesMapR.getStatWithMediansAndFreq(crimeFacts, offenseCodesBroadcast)

    statWithMediansAndFreq.coalesce(1).write.format("parquet").save(outputFolder)
  }
}
