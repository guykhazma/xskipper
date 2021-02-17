/*
 * Copyright 2021 IBM Corp.
 * SPDX-License-Identifier: Apache-2.0
 */

package io.xskipper.search

import io.xskipper.configuration.XskipperConf
import io.xskipper.metadatastore.{ClauseTranslator, TranslationUtils}
import io.xskipper.search.expressions.IcebergExpressionTranslator
import io.xskipper.search.filters.MetadataFilterFactory
import io.xskipper.utils.Utils
import io.xskipper.{Registration, Xskipper}
import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.iceberg.{IcebergUtils, StructLike, Table}
import org.apache.iceberg.expressions.{Evaluator, Expression => IcebergExpression}
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.Expression

import scala.collection.JavaConverters._
import scala.concurrent.Await
import scala.concurrent.duration._

/**
  * Prototype of Iceberg integration with Xskipper
  * @param table the iceberg table to filter
  * @param filter the filter to check against the metadata
  */
class IcebergDataSkippingFileFilter(table: Table,
                                    filter: IcebergExpression)
                                      extends Evaluator(filter) {
  val logger = Logger.getLogger(this.getClass.getSimpleName())
  val metadataStoreManager = Registration.getActiveMetadataStoreManager()
  val params = Xskipper.getConf().asScala.toMap

  // init to the default value from the JVM conf
  // once an instance is created the configuration is modified if needed
  private val TIMEOUT = XskipperConf.getConf(XskipperConf.XSKIPPER_TIMEOUT)

  protected lazy val metadataHandler =
    metadataStoreManager.getOrCreateMetadataHandle(SparkSession.active, table.name())

  // indicates whether the current query is relevant for skipping.
  // i.e - it has indexed files and a metadata query can be generated
  private var isSkippable = false

  private var required: Set[String] = _
  private var indexed: Set[String] = _

  init(filter)

  def init(dataFilters: IcebergExpression): Unit = {
    // translate the iceberg expression to dataFilters
    init(Seq(IcebergExpressionTranslator.translate(dataFilters)), Seq.empty,
      Registration.getCurrentMetadataFilterFactories(),
      Registration.getCurrentClauseTranslators())
  }

  /**
    * Filters the partition directory by removing unnecessary objects from each partition directory
    *
    * @param dataFilters query predicates for actual data columns (not partitions)
    * @param partitionFilters the partition predicates from the query
    * @param metadataFilterFactories a sequence of MetadataFilterFactory to generate filters
    *                                according to the index on the dataset
    * @param clauseTranslators a sequence of ClauseTranslators to be applied on the clauses
    * @return a sequence of PartitionDirectory after filtering the unnecessary objects
    *         using the metadata
    */
  def init(
            dataFilters: Seq[Expression],
            partitionFilters: Seq[Expression],
            metadataFilterFactories: Seq[MetadataFilterFactory],
            clauseTranslators: Seq[ClauseTranslator]): Unit = {
    // set the metadata store params - needed when we infer the metadata location from hive table/db
    metadataHandler.setParams(params)
    // get the indexes
    val indexDescriptor = metadataHandler.getIndexes().distinct

    if (!indexDescriptor.isEmpty) {
      // get the abstract query
      val filters = metadataFilterFactories.map(_.getFilters(indexDescriptor)).flatten.distinct
      val abstractFilterQuery = MetadataQueryBuilder.getClause(dataFilters, filters)

      abstractFilterQuery match {
        case Some(abstractQuery) =>
          // translate the query to the metadatastore representation
          val translatedQuery = TranslationUtils.getClauseTranslation[Any](
            metadataStoreManager.getType, abstractQuery, clauseTranslators)
          translatedQuery match {
            case Some(queryInstance) =>
              isSkippable = true // translation succeeded so dataset is skippable
              logger.info(s"Filtering partitions using " +
                s"${metadataStoreManager.getType.toString} backend")
              logger.info("Getting all indexed files and required files")
              val indexedFut = metadataHandler.getAllIndexedFiles()
              val requiredFut = metadataHandler.getRequiredObjects(queryInstance)
              indexed = Await.result(indexedFut, TIMEOUT minutes)
              required = Await.result(requiredFut, TIMEOUT minutes)
              if (logger.isTraceEnabled()) {
                (indexed -- required).foreach(f => logger.trace(s"""${f}--->SKIPPABLE!"""))
              }
            case _ =>
              logger.info("No translation for the abstract query => no skipping")
          }
        case _ =>
          logger.info("No abstract query generated => no skipping")
      }
    } else {
      logger.info("Dataset is not indexed => no skipping")
    }
  }

  /**
    * Returns true if the current file is required for the given query by checking
    * if it is present in the required files or not indexed
    *
    * @param dataFile the file status to check
    * @return true if the file is required, false otherwise
    */
  override def eval(dataFile: StructLike): Boolean = {
    val p = IcebergUtils.getGenericDataFilePath(dataFile)
    // TODO: this is a dummy file status
    val fs = new FileStatus(0, false, 0, 0, 0, 0, null,
      null, null, new Path(p))
    val id = Utils.getFileId(fs)

    // the file isRequired if it is in the required files or not indexed
    val res = required.contains(id) || !indexed.contains(id)
    logger.info(s"${id} ${if (res) "" else "--------> SKIPPED!"}")
    res
  }
}
