/*
 * Copyright 2021 IBM Corp.
 * SPDX-License-Identifier: Apache-2.0
 */

package io.xskipper.index.execution

import io.xskipper.XskipperException
import io.xskipper.configuration.XskipperConf
import io.xskipper.index.execution.parquet.ParquetMinMaxIndexing
import io.xskipper.index.metadata.MetadataType
import io.xskipper.index.{Index, IndexField, MinMaxIndex}
import io.xskipper.metadatastore._
import io.xskipper.utils.Utils
import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.iceberg.FindFiles
import org.apache.iceberg.spark.source.SparkTable
import org.apache.spark.internal.Logging
import org.apache.spark.sql.execution.datasources.v2.{DataSourceV2ScanRelation, FileTable}
import org.apache.spark.sql.execution.datasources.{HadoopFsRelation, LogicalRelation}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.util.SizeEstimator

import collection.JavaConverters._
import scala.collection.mutable
import scala.collection.parallel.ForkJoinTaskSupport
import scala.concurrent.Await
import scala.concurrent.duration._
import scala.math.max

/**
  * A Helper class which collects the indexes and use a [[MetadataHandle]] to upload the metadata
  */
object MetadataProcessor {
  def apply(spark: SparkSession, uri: String, metadataHandler: MetadataHandle): MetadataProcessor =
    new MetadataProcessor(spark, uri, metadataHandler)

  // allows us to collect info like last_modified from a dataframe
  def listFilesFromDf(df: DataFrame): Seq[FileStatus] = {
    df.queryExecution.optimizedPlan.collect {
      case l@LogicalRelation(hfs: HadoopFsRelation, output, catalogTable, isStreaming) =>
        hfs.location.listFiles(Seq.empty, Seq.empty).flatMap { part =>
          part.files
        }
      // Iceberg integration
      case l@DataSourceV2ScanRelation(sparkTable: SparkTable, _, _) =>
        FindFiles.in(sparkTable.table()).collect().asScala.map(dataFile => {
          new FileStatus(dataFile.fileSizeInBytes(), false, 0, 0,
            0, 0, null,
            null, null, new Path(dataFile.path().toString))
        })
      case DataSourceV2ScanRelation(table: FileTable, _, _) =>
        // not using allFiles since it returns also empty files which are not used
        // since Spark only calls list files during query processing
        table.fileIndex.listFiles(Seq.empty, Seq.empty).flatMap { part =>
          part.files
        }
    }.flatten
  }
}

/**
  * A Helper class which collects the indexes and use a [[MetadataHandle]] to upload
  * the metadata
  *
  * @param spark [[org.apache.spark.sql.SparkSession]] instance for processing
  * @param uri the URI of the dataset
  * @param metadataHandle a [[MetadataHandle]] instance to be used for saving the metadata
  */
class MetadataProcessor(spark: SparkSession, uri: String, metadataHandle: MetadataHandle)
  extends Logging with Serializable {

  val tableIdentifier = Utils.getTableIdentifier(uri)
  private val PARALLELISM =
    XskipperConf.getConf(XskipperConf.XSKIPPER_INDEX_CREATION_PARALLELISM)
  logInfo(s"Parallelism set to ${PARALLELISM}")
  private val MIN_CHUNK_SIZE =
    XskipperConf.getConf(XskipperConf.XSKIPPER_INDEX_CREATION_MIN_CHUNK_SIZE)
  logInfo(s"Min chunk size set to ${MIN_CHUNK_SIZE}")
  private val TIMEOUT = XskipperConf.getConf(XskipperConf.XSKIPPER_TIMEOUT)
  logInfo(s"Timeout set to ${TIMEOUT}")
  private val DRIVER_MEMORY_FRACTION =
    XskipperConf.getConf(XskipperConf.XSKIPPER_INDEX_DRIVER_MEMORY_FRACTION)
  logInfo(s"Driver memory fraction set to ${DRIVER_MEMORY_FRACTION}")

  /**
    * Collects the indexes and them to the metadatastore using the [[MetadataHandle]]
    *
    * @param format the format to be used when reading each object
    * @param options the options to be used when reading each object
    *                Note: all objects are assumed to have the same options and format.
    * @param indexes a sequence of indexes that will be applied on the indexed dataset
    * @param fileIds a sequence of (String, String) where the first string is the file name
    *                and the second is the fileID
    * @param schema (optional) the expected schema (since we are reading object by object the
    *               schema can be provided according to the full dataframe)
    * @param isRefresh indicates whether the operation is a refresh
    *
    */
  def analyzeAndUploadMetadata(
    format: String,
    options: Map[String, String],
    indexes: Seq[Index],
    fileIds: Seq[(String, String)],
    schema: Option[StructType],
    isRefresh: Boolean = false) : Unit = {
    logInfo("Generating objects metadata...")

    // Initialize metadata per the dataset if needed
    if (!isRefresh) {
      metadataHandle.initMetadataUpload(indexes)
    }

    // Run minmax optimization for Parquet to read min/max stats from footers
    if (
      XskipperConf.getConf(XskipperConf.XSKIPPER_PARQUET_MINMAX_INDEX_OPTIMIZED) &&
      // check that all indexes are min max and numeric
      format.equalsIgnoreCase("parquet") &&
      indexes.forall(index => index.isInstanceOf[MinMaxIndex] &&
        // Optimization is only relevant for non-decimal numerical columns
      index.getIndexCols.forall(col => col.dataType.isInstanceOf[NumericType] &&
      !col.dataType.isInstanceOf[DecimalType]))) {
        logInfo("Index request include only min/max indexes on parquet files - " +
          s"running in parallel optimized mode in groups of" +
          s" ${metadataHandle.getUploadChunkSize()}")
        // index in batches according to the metadatastore batch size
        fileIds.grouped(metadataHandle.getUploadChunkSize()).foreach(group => {
          ParquetMinMaxIndexing.parquetMinMaxParallelIndexing(metadataHandle,
            options, indexes, group, isRefresh, spark)
        })
    } else {
      var objectUploaded = 0L
      val indexCols = indexes.flatMap(index => index.getIndexCols)

      // Correlated to collectMD indexes order (for schema)
      val optIndexes = indexes.filter(_.isOptimized)
      val nonOptIndexes = indexes.filter(!_.isOptimized)

      // get driver memory in order to calculate the maximum possible chunk size
      val driverMemory =
        Utils.memoryStringToMb(spark.conf.get("spark.driver.memory", "4g")).toLong << 20
      // The metadata data is collected in chunks starting with a chunk of Conf and up to max
      // size of metadataStore.getUploadChunkSize()
      // This is done to ensure there is at least some work done in a timeframe of 60 minutes
      val maxChunkSize = max(metadataHandle.getUploadChunkSize(), 1)
      // the maximum chunk sized will change according to the size of each metadata row
      // the change may reduce the chunk size in order to account for the available memory in the
      // driver. in any case the maximum will remain the value set by
      // metadataStore.getUploadChunkSize()
      var currMaxChunkSize = maxChunkSize
      logInfo(s"Init currMaxChunkSize to ${currMaxChunkSize}")
      var currChunkSize = max(MIN_CHUNK_SIZE, 1)
      logInfo(s"Init currChunkSize to ${currChunkSize}")

      // using pool to limit the number of threads created when calling chunk.par
      // Note that using par on a sequence of dataframes results in  a high memory consumption
      // and a the same dataframe reader instance cannot be used in parallel collection.
      // Therefore, we use the format and options in order to read the dataframe inside the
      // collectMD function.
      val forkJoinPool = new scala.concurrent.forkjoin.ForkJoinPool(PARALLELISM)
      var index = 0
      while (index < fileIds.length)
      {
        // get next chunk
        val chunk_par = fileIds.slice(index, index + currChunkSize).par
        // collect the metadata in parallel
        chunk_par.tasksupport = new ForkJoinTaskSupport(forkJoinPool)
        val chunk_data = chunk_par.map {
          case (fname: String, fId: String) =>
            collectMD(fId, fname, optIndexes, nonOptIndexes, format, options, schema, indexCols)
        }.toList
        val metadataRDD = spark.sparkContext.parallelize(chunk_data)
        // Upload metadata to store incrementally - it's important to pass the indexes in the right
        // order as the schema might depend on that
        metadataHandle.uploadMetadata(metadataRDD, optIndexes ++ nonOptIndexes, isRefresh)
        objectUploaded += chunk_par.size
        logInfo(s"MetaDataProcessor: Indexed ${objectUploaded} objects to metadatastore")
        // update chunk size and index for next iteration
        index += chunk_par.size
        // try to adjust the max chunk size by estimating the size of metadata row
        try {
          // calculate the maximum available chunk size according to the latest iteration
          val metadataRowSizeEstimation =
            SizeEstimator.estimate(chunk_data).toDouble / chunk_data.size
          // adjust max chunk size according to the available driver memory
          // we enable to use up to DRIVER_MEMORY_FRACTION of the memory
          // adjust after every iteration to avoid being stuck at 1 if possible
          // note that currMaxChunkSize is at least 1 and at most metadataStore.getUploadChunkSize()
          currMaxChunkSize =
            math.min(maxChunkSize,
              math.max(1,
                (DRIVER_MEMORY_FRACTION * (driverMemory / metadataRowSizeEstimation)).toInt))
          logInfo(s"Adjusted currMaxChunkSize to ${currMaxChunkSize}")
        } catch {
          // catch all exceptions - for cases where size estimation may not work
          case e: Exception =>
            logInfo("Failed to adjust currMaxChunkSize with size estimation", e)
            logInfo("Falling back to adjusting by number of indexes")
            currMaxChunkSize = max(metadataHandle.getUploadChunkSize() / indexes.size, 1)
            logInfo(s"Adjusted currMaxChunkSize to ${currMaxChunkSize}")
        }
        currChunkSize = if (currChunkSize < currMaxChunkSize) {
          val res = math.min(currChunkSize*2, currMaxChunkSize)
          res
        } else {
          currMaxChunkSize
        }
        logInfo(s"Adjusted currChunkSize to ${currChunkSize}")
      }
      forkJoinPool.shutdown()
    }

    // Steps to take after uploading all dataset's indexes meatadata
    metadataHandle.finalizeMetadataUpload()
  }

  /**
    * Collects the metadata for a given file
    *
    * @param fid the filename (full URI location) to be read
    * @param fname the file name
    * @param optIndexes the list of indexes with optimization to be collected
    * @param nonOptIndexes the list of indexes with no optimization to be collected
    * @param format the format to be used when reading each object
    * @param options the options to be used when reading each object
    * @param schema (optional) when specified no infer schema will be used for reading
    *               the [[DataFrame]]
    * @param indexCols the sequence of columns needed for creating the indexes
    * @return a sequence of [[MetadataType]] corresponding to the sequence of indexes requested
    */
  private def collectMD(
             fid: String,
             fname: String,
             optIndexes: Seq[Index],
             nonOptIndexes: Seq[Index],
             format: String, options: Map[String, String],
             schema: Option[StructType],
             indexCols: Seq[IndexField]): Row = {
    val indexedDataDF = {
      // Using "as" to flatten nested fields
      import org.apache.spark.sql.functions._
      fileToDF(fname, format, options, schema)
        .select(indexCols.map(c => col(c.name).as(c.name)): _*)
    }
    logTrace("collectMD of : " + fname)

    var metaData: mutable.Buffer[MetadataType] = mutable.Buffer.empty[MetadataType]

    // first run all indexes with optimizations
    metaData ++= optIndexes.map(index => {
      logTrace("Optimize index collect : " + index.toString)
      index.optCollectMetaData(fname, indexedDataDF, format, options)
    })

    logTrace("General index collect : " + nonOptIndexes)
    if (!nonOptIndexes.isEmpty) {
      val base = nonOptIndexes.map(_.generateBaseMetadata)
      // using broadcast variable to avoid each task sending this variable
      val zipIndexes = spark.sparkContext.broadcast(nonOptIndexes.zipWithIndex)

      metaData ++= indexedDataDF.rdd.treeAggregate(base)(
        // update according to each index function
        (accuMetaDataList, currRowVal) => {
          zipIndexes.value.map {
            case (index, i) => index.reduce(accuMetaDataList(i), index.getRowMetadata(currRowVal))
          }
        },
        (accuMetaDataList1, accuMetaDataList2) => {
          zipIndexes.value.map {
            case (index, i) => index.reduce(accuMetaDataList1(i), accuMetaDataList2(i))
          }
        })

      // destroy broadcast value
      zipIndexes.destroy()
    }
    // create the row
    Row.fromSeq(Seq(fid) ++ metaData)
  }

  def prepareForRefresh(indexes: Seq[Index]): Unit = {
    metadataHandle.getMdVersionStatus() match {
      case MetadataVersionStatus.DEPRECATED_SUPPORTED
        | MetadataVersionStatus.DEPRECATED_UNSUPPORTED =>
        if (!metadataHandle.isMetadataUpgradePossible()) {
          throw new XskipperException("cannot upgrade metadata")
        }
        logInfo(s"Upgrading Metadata for $tableIdentifier")
        metadataHandle.upgradeMetadata(indexes)
        logInfo(s"Done upgrading Metadata for $tableIdentifier")
      case MetadataVersionStatus.TOO_NEW =>
        throw new XskipperException("cannot upgrade from a higher version")
      case _ =>
    }
  }


  /**
    * Collects the list of files that needs to be indexed
    * A file needs to be indexed if:
    * 1. It is a new file that was not indexed before
    * 2. It is an indexed file which changed since it was indexed
    *
    * @param files The list of files the will be compared against the existing indexed files
    * @param isRefresh indicates whether this is a refresh operation or not, in case this is not a
    *                  refresh operation assuming no indexed files exits
    * @return Sequence of (String, String) where the first string is the file name and the second
    *         is the fileID for all of new/modified files,
    *         Sequence of files to be removed from the metadatastore (since they were updated)
    */
  def collectNewFiles(files: Seq[FileStatus],
                      isRefresh: Boolean): (Seq[(String, String)], Seq[String]) = {
    // collect indexed files IDs
    var allIndexedFiles = Set.empty[String]
    if (isRefresh) {
      val asyncAllFilesRequest = metadataHandle.getAllIndexedFiles()
      // we give a TIMEOUT minutes timeout since we block on this request
      allIndexedFiles = Await.result(asyncAllFilesRequest, TIMEOUT minutes)
    }
    // collect the dataframe files
    val fileIds = files.map{ fs => (fs.getPath.toString, Utils.getFileId(fs)) }.view

    // choose only records from files that are not indexed in the metadata store
    // filtering will leave only new objects or objects that have been updated since
    // the last indexing (and therefore the current fileID will be different than the one
    // indexed in the metadatastore)
    val newOrModifiedFileIds = fileIds.filter {
      case (fileLocation: String, fileID: String) =>
        !allIndexedFiles.contains(fileID)
    }

    // getting the list of fileIDs to be removed since the object has been updated
    // or deleted since last indexing
    val removedFilesIds = allIndexedFiles.diff(fileIds.unzip._2.toSet)

    (newOrModifiedFileIds, removedFilesIds.toSeq)
  }

  /**
    * Given a filename (full URI location) return a the dataframe that results from reading.
    *
    * @param filename the filename (full URI location) to be read
    * @param schema (optional) when specified no infer schema will be used for reading the dataframe
    * @param format the format to be used when reading each object
    * @param options the options to be used when reading each object
    * @return a dataframe resulting from reading the file using the given reader
    */
  private def fileToDF(filename: String, format: String,
               options: Map[String, String], schema : Option[StructType]): DataFrame = {
    // if schema is specified read without inferring schema
    schema match {
      case Some(schemaInstance) => spark.read.format(format).options(options)
        .option("inferSchema", "false").schema(schemaInstance).load(filename)
      case _ => spark.read.format(format).options(options)
        .option("inferSchema", "true").load(filename)
    }
  }

  /**
    * Removes the metadata for a given list of files
    *
    * @param files the list of files to remove metadata for
    */
  def removeMetadataForFiles(files: Seq[String]): Int = {
    files.grouped(metadataHandle.getDeletionChunkSize())
      .foreach(group => metadataHandle.removeMetaDataForFiles(group))
    files.size
  }
}
