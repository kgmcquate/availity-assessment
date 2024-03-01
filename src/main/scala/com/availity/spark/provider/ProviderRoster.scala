package com.availity.spark.provider

import org.apache.spark.sql.{SparkSession, Dataset, Encoders}
import org.apache.spark.sql.types.{StructType, DateType, StringType}

import org.apache.spark.sql.functions.{count, lit, array, collect_list, col, month, avg, concat}

case class Provider(provider_id: Long, provider_specialty: String, first_name: String, middle_name: String, last_name: String)
case class Visit(visit_id: Long, provider_id: Long, visit_date: java.time.LocalDate)

object ProviderRoster  {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder.master("local").getOrCreate()

    val sourcePath = "s3://data-zone-117819748843-us-east-1/provider_roster/raw/"
    val targetPath = "s3://data-zone-117819748843-us-east-1/provider_roster/processed/"

    val providers: Dataset[Provider] = 
      spark
        .read
        .format("csv")
        .option("delimiter",  "|")
        .option("header",  true)
        .schema(Encoders.product[Provider].schema)
        .load(sourcePath + "providers.csv")
        .as[Provider]

    val visits: Dataset[Visit] = 
      spark
        .read
        .format("csv")
        .option("delimiter",  ",")
        .option("header",  false)
        .schema(Encoders.product[Visit].schema)
        .load(sourcePath + "visits.csv")
        .as[Visit]
      
    /*
    Problem 1
    Given the two data datasets, calculate the total number of visits per provider. The resulting set should contain the provider's ID, name, specialty, along with the number of visits. Output the report in json, partitioned by the provider's specialty.
    */
    
    val providerVisitsCount =
      visits
      .groupBy("provider_id")
      .agg(
        count(lit(1)).alias("visit_count")
      )
      .join(
        providers,
        Seq("provider_id"),
        "outer"
      )
      .withColumn(
        "provider_name",
        concat(col("first_name"), lit(" "), col("middle_name"), lit(" "), col("last_name"))
      )
      

    // providerVisitsCount.show()

    providerVisitsCount
      .select(
        col("provider_id"),
        col("provider_name"),
        col("provider_specialty"),
        col("visit_count")
      )
      .write
      .format("json")
      .partitionBy("provider_specialty")
      .mode("overwrite")
      .save(targetPath + "provider_visits_count/")

    /*
    Problem 2
    Given the two datasets, calculate the total number of visits per provider per month. The resulting set should contain the provider's ID, the month, and total number of visits. Output the result set in json.
    */
    val monthlyProviderVisitsCount = 
      visits
        .withColumn("visit_month", month(col("visit_date")))
        .groupBy("provider_id", "visit_month")
        .agg(
          count(lit(1)).alias("visit_count")
        )
        .join(
          providers,
          Seq("provider_id"),
          "outer"
        )

    // monthlyProviderVisitsCount.show()

    monthlyProviderVisitsCount
      .selectExpr(
        "provider_id",
        "visit_month",
        "visit_count"
      )
      .write
      .format("json")
      // .partitionBy("provider_specialty", "visit_month") //TODO could be useful to partition and sort
      .mode("overwrite")
      .save(targetPath + "provider_visits_count_monthly/")

  }
}