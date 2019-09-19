package com.example.BostonCrimesMap

/**
 *
 */

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{avg, collect_list, concat_ws, count, first, split, sum}

object BostonCrimesMapR {
  /**
   *
   */
  def getStatWithMediansAndFreq(crimeFacts: DataFrame, offenseCodesBroadcast: DataFrame): DataFrame = {

    val spark = SparkSession.getActiveSession.get
    import spark.implicits._

    val districtsStatsBase = crimeFacts
      .filter("DISTRICT IS NOT NULL")
      .groupBy(crimeFacts("DISTRICT"))
      .agg(
        count(crimeFacts("INCIDENT_NUMBER")).alias("crimes_total"),
        avg(crimeFacts("Lat")).alias("lat"),
        avg(crimeFacts("Long")).alias("lng")
      )
      .orderBy($"DISTRICT".asc)

    val districtsStatsByMonthes = crimeFacts
      .filter("DISTRICT IS NOT NULL")
      .groupBy(crimeFacts("DISTRICT"), crimeFacts("YEAR"), crimeFacts("MONTH"))
      .count()
      .orderBy(crimeFacts("DISTRICT").asc, crimeFacts("YEAR").asc, crimeFacts("MONTH").asc)

    districtsStatsByMonthes.createOrReplaceTempView("MonthesCount")
    val monthesCountMedian = spark.sql("""SELECT DISTRICT, percentile_approx(count, 0.5, 100) AS crimes_monthly
FROM MonthesCount GROUP BY DISTRICT""")

    val districtsWithCrimeTypes = crimeFacts
      .filter("DISTRICT IS NOT NULL")
      .join(offenseCodesBroadcast, $"CODE" === $"OFFENSE_CODE")
      .withColumn("_tmp", split($"NAME", " - "))
      .select(
        $"DISTRICT",
        $"_tmp".getItem(0).as("CrimeType")
      )
      .drop("_tmp")
      .groupBy($"DISTRICT", $"CrimeType")
      .count()
      .orderBy($"DISTRICT".asc, $"CrimeType".asc)

    val freqWin = Window.partitionBy($"DISTRICT").orderBy($"count".desc).rowsBetween(0, 2)
    val statWithCrimeTypes = districtsWithCrimeTypes
      .withColumn(
        "frequent_crime_types",
        concat_ws(", ", collect_list("CrimeType") over freqWin)
      )
      .withColumn(
        "sum_frequent_counts",
        (sum($"count") over freqWin)
      )
      .select($"DISTRICT", $"frequent_crime_types", $"sum_frequent_counts")
      .orderBy($"sum_frequent_counts".desc)

    val freqWin2 = Window.partitionBy($"DISTRICT").orderBy($"sum_frequent_counts".desc)
    val firstFreqCrimeTypes = first($"frequent_crime_types").over(freqWin2)
    val firstSumFreqCounts = first($"sum_frequent_counts").over(freqWin2)
    val topWithCrimeTypes = statWithCrimeTypes
      .select(
        $"DISTRICT",
        firstFreqCrimeTypes as "frequent_crime_types",
        firstSumFreqCounts as "sum_frequent_counts"
      )
      .distinct

    val statWithMedians = districtsStatsBase
      .join(
        monthesCountMedian,
        districtsStatsBase("DISTRICT") === monthesCountMedian("DISTRICT"),
        "inner"
      )
      .select(districtsStatsBase("DISTRICT"), $"crimes_total", $"crimes_monthly", $"lat", $"lng")

    val statWithMediansAndFreq = statWithMedians.join(
      topWithCrimeTypes,
      statWithMedians("DISTRICT") === topWithCrimeTypes("DISTRICT"),
      "inner"
    )
      .select(statWithMedians("DISTRICT"), $"crimes_total", $"crimes_monthly", $"frequent_crime_types", $"lat", $"lng")

    statWithMediansAndFreq
  }
}
