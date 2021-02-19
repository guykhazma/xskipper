/*
 * Copyright 2021 IBM Corp.
 * SPDX-License-Identifier: Apache-2.0
 */

package io.xskipper

import org.apache.log4j.{Level, LogManager}
import org.apache.spark.sql.SparkSession
import io.xskipper.implicits._

object IcebergRunner {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("Iceberg Runner")
      .config("spark.master", "local[*]") // comment out to run in production
      .config("spark.sql.extensions",
        "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
      .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog")
      .config("spark.sql.catalog.spark_catalog.type", "hive")
      .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
      .config("spark.sql.catalog.local.type" , "hadoop")
      .config("spark.sql.catalog.local.warehouse", "/tmp/iceberg")
      // Setting the external file filter fully qualified name
      .config("spark.hadoop.iceberg.read.fileFilter.impl",
        "io.xskipper.search.IcebergDataSkippingFileFilter")
      .getOrCreate()

    val reindex = false

    // set debug log level specifically for xskipper search package to view skipped files in the log
    LogManager.getLogger("io.xskipper.search").setLevel(Level.DEBUG)

    val md_base_location = s"/tmp/metadata"

    // Configuring the JVM wide parameters
    val conf = Map(
      "io.xskipper.parquet.mdlocation" -> md_base_location,
      "io.xskipper.parquet.mdlocation.type" -> "EXPLICIT_BASE_PATH_LOCATION")
    Xskipper.setConf(conf)

    // Indexing sample
    if (reindex) {
      // create Xskipper instance for the sample dataset
      val xskipper = new Xskipper(spark, "local.db.table")

      // remove existing index if needed
      if (xskipper.isIndexed()) {
        xskipper.dropIndex()
      }

      xskipper.indexBuilder()
        .addValueListIndex("city")
        .addBloomFilterIndex("vid")
        .build()
        .show(false)
    }

    spark.enableXskipper()

    spark.sql("select * from local.db.table where city = 'Vidauban'").show(false)
  }
}
