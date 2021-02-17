/*
 * Copyright 2021 IBM Corp.
 * SPDX-License-Identifier: Apache-2.0
 */

package io.xskipper.metadatastore.parquet

import com.ibm.metaindex.metadata.index.types.util.BloomFilterTransformer
import io.xskipper.Registration
import io.xskipper.index.{BloomFilterIndex, Index}
import io.xskipper.metadatastore.MetadataVersionStatus._
import org.apache.spark.internal.Logging
import org.apache.spark.sql._
import org.apache.spark.sql.execution.datasources.parquet.SparkToParquetSchemaConverter
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.xskipper.utils.DataFrameColumnUpgrader.applyUpgradeDescriptor
import org.apache.spark.sql.types.{Metadata, _}
import org.apache.spark.{SparkException, sql}

import scala.collection.JavaConverters._
import scala.reflect.runtime.universe._


case class EncryptionDescriptor(columnKeyListString: String,
                                footerLabel: String, plaintextFooterEnabled: Boolean)

case class UpgradeDescriptor(origColName: String,
                             upgradeColumn: Column,
                             newColName: String,
                             newMetadata: Metadata)

object ParquetUtils extends Logging {

  /**
    * returns the column name for the specified index and version
    *
    * @param idx     the index for which the column name needs to created
    * @param version version number, the metadata spec of which will determine
    *                the column name
    * @return
    */
  def getColumnName(idx: Index,
                    version: Long = ParquetMetadataStoreConf.PARQUET_MD_STORAGE_VERSION): String = {
    getColumnNameForCols(idx.getCols, idx.getName, version)
  }

  def getColumnNameForCols(cols: Seq[String],
                           idxName: String,
                           version: Long
                           = ParquetMetadataStoreConf.PARQUET_MD_STORAGE_VERSION): String = {
    version match {
      case 0 => cols.mkString(":") + "_" + idxName
      case 1 => cols.mkString("_") + "_" + idxName
      case x if x >= 2 && x <= ParquetMetadataStoreConf.PARQUET_MD_STORAGE_VERSION =>
        val cleanedColNames = cols.map(colName => colName
          // replace all dots with $#$, duplicate existing # to make mapping 1:1
          .replace("#", "##").replace(".", "$#$"))
        val lenDescriptors = cleanedColNames.map(_.length.toString).mkString("-")
        cleanedColNames.mkString("_") + "_" + idxName + "_" + lenDescriptors
      case x if x > ParquetMetadataStoreConf.PARQUET_MD_STORAGE_VERSION =>
        throw new ParquetMetaDataStoreException(s"Version $x is greater than current version" +
          s" ${ParquetMetadataStoreConf.PARQUET_MD_STORAGE_VERSION}")
      case x if x < 0 =>
        throw new ParquetMetaDataStoreException(s"Negative Version number ($x)")
    }
  }

  /**
    * retrieves the version number from a Metadata DataFrame, returns 0 if the
    * version is not explicitly defined (files without version number
    * are implicitly declared version 0).
    * the function assumes the `obj_name` column exists in the schema.
    */
  def getVersion(df: DataFrame): Long = {
    getVersion(df.schema)
  }


  /**
    * retrieves the version number from a metadata DataFrame Schema, returns 0 if the
    * version is not explicitly defined (files without version number
    * are implicitly declared version 0).
    * the function assumes the `obj_name` column exists in the schema.
    *
    * @param schema - the schema of the metadata df
    */
  def getVersion(schema: StructType): Long = {
    val objNameMeta = schema.apply("obj_name").metadata
    objNameMeta.contains("version") match {
      case true => objNameMeta.getLong("version")
      // implicitly decide version 0 if no version on the schema
      case false => 0L
    }
  }

  /**
    * Upgrades the schema to comply with the most current version.
    * note that only the schema is upgraded (including the column metadata and version),
    * but the column types and order are not changed.
    *
    * @param df      the RAW metadata DataFrame
    * @param indexes indexes (with the colsMap), must comply with the schema
    * @return the upgraded schema
    */
  private[parquet] def extractSchema(df: DataFrame, indexes: Seq[Index]): StructType = {
    extractSchema(df.schema, indexes)
  }

  /**
    * Upgrades the schema to comply with the most current version.
    * note that only the schema is upgraded (including the column metadata and version),
    * but the column types and order are not changed.
    *
    * @param schema  original RAW schema
    * @param indexes indexes (with the colsMap), must comply with the schema
    * @return
    */
  private[parquet] def extractSchema(schema: StructType, indexes: Seq[Index]): StructType = {
    // no need to do anything if the version is the same as the current one
    if (getVersion(schema) == ParquetMetadataStoreConf.PARQUET_MD_STORAGE_VERSION) {
      return schema
    }

    val tableIdentifier = getTableIdentifier(schema) match {
      case Some(tid) => tid
      case _ =>
        throw ParquetMetaDataStoreException("could not extract table identifier")
    }

    val newSchema = ParquetUtils.extractEncryptionDescriptor(schema) match {
      case Some(EncryptionDescriptor(_, footerLabel, plaintextFooterEnabled)) =>
        createDFSchema(indexes, true,
          tableIdentifier, Some(footerLabel), plaintextFooterEnabled)
      case None => createDFSchema(indexes, true, tableIdentifier, None, false)
    }
    newSchema
  }

  private[parquet] def getTableIdentifier(schema: StructType): Option[String] = {
    schema
      .collectFirst({
        // old metadata (version < 1), the tableIdentifier is stored with the indexes
        case field: StructField if field.metadata.contains("index")
          && field.metadata.getMetadata("index").contains("tableIdentifier") =>
          field.metadata.getMetadata("index").getString("tableIdentifier")
        // new md (version >=1 ), stored in the obj_name column
        case field: StructField if field.metadata.contains("tableIdentifier") =>
          field.metadata.getString("tableIdentifier")
      })
  }

  /**
    * Generates the writer options used when writing the metadata.
    * these extra options are passed via [[DataFrameWriter.options()]].
    * this includes the key (columns + footer) when encryption is on,
    * these (if applicable) are extracted from the metadata of the `obj_name` column
    * of the input df, under "encryption" key
    */
  private[parquet] def getDFWriterOptions(df: DataFrame): Map[String, String] = {
    getDFWriterOptions(df.schema)
  }

  /**
    * Generates the writer options used when writing the metadata.
    * these extra options are passed via [[DataFrameWriter.options()]].
    * this includes the key (columns + footer) when encryption is on,
    * these (if applicable) are extracted from the metadata of the `obj_name` column
    * of the input df, under "encryption" key
    *
    * NOTE: we always write dfs of the most current version, so
    * we don't need to think about versions here - everything is up to date.
    */
  private[parquet] def getDFWriterOptions(schema: StructType): Map[String, String] = {
    val res = scala.collection.mutable.HashMap[String, String]()
    extractEncryptionDescriptor(schema) match {
      case Some(EncryptionDescriptor(columnKeyListString,
      footerLabel,
      plaintextFooterEnabled)) => {
        res.put(key = ParquetMetadataStoreConf.PARQUET_COLUMN_KEYS_SPARK_KEY,
          value = columnKeyListString)
        res.put(key = ParquetMetadataStoreConf.PARQUET_FOOTER_KEY_SPARK_KEY,
          value = footerLabel)
        // as a convention (also expressed in the spec), can omit plaintextFooter definition
        // if it's false
        if (plaintextFooterEnabled) {
          res.put(key = ParquetMetadataStoreConf.PARQUET_PLAINTEXT_FOOTER_SPARK_KEY,
            value = plaintextFooterEnabled.toString)
        }
      }
      case _ =>
    }

    res.toMap
  }

  private[parquet] def extractEncryptionDescriptor(schema: StructType)
  : Option[EncryptionDescriptor] = {
    // it's assumed that obj_name is a column in this schema.
    val metaData = schema.apply("obj_name").metadata
    if (!metaData.contains("encryption")) return None
    val encMeta = metaData.getMetadata("encryption")

    // the logic regarding encryption is as follows:
    // encryption is turned ON iff at least 1 index is marked encrypted.
    // if encryption is ON, then the footer key must be set regardless
    // of plaintext footer being set or not (used for footer signing by PME
    // even if the footer itself is left plaintext).
    // in that case, if plaintext footer is set to ON (default) then the footer
    // and the obj_name column are both encrypted with the footer key.
    // if plaintext footer is set to ON - the obj_name is still encrypted
    // and the footer is in plaintext mode. we encrypt the obj_name in that case
    // mainly for tamper proofing (the footer is tamper-proofed even in plaintext footer mode)
    // any other config (including a case where no index is encrypted but a footer key is set)
    // is illegal.
    // note that the part regarding the obj_name column is taken care of while creating
    // the master metadata (specifically, the column key list string).

    // the column keys must be set, as "encryption" meta is set
    assert(encMeta.contains(ParquetMetadataStoreConf.PARQUET_COLUMN_KEYS_SPARK_KEY))
    val columnKeyListString = encMeta
      .getString(ParquetMetadataStoreConf.PARQUET_COLUMN_KEYS_SPARK_KEY)

    // the footer key must also be set in the meta
    assert(encMeta.contains(ParquetMetadataStoreConf.PARQUET_FOOTER_KEY_SPARK_KEY))
    // the footer label
    val footerLabel = encMeta.getString(ParquetMetadataStoreConf.PARQUET_FOOTER_KEY_SPARK_KEY)

    // if the key for plaintext footer does not exist then it's implicitly false
    val plaintextFooterEnabled =
      encMeta.contains(ParquetMetadataStoreConf.PARQUET_PLAINTEXT_FOOTER_SPARK_KEY) &&
        encMeta.getString(ParquetMetadataStoreConf.PARQUET_PLAINTEXT_FOOTER_SPARK_KEY).toBoolean
    Some(EncryptionDescriptor(columnKeyListString, footerLabel, plaintextFooterEnabled))
  }

  /**
    * Creates the Metadata for a single Index
    */
  private[parquet] def createIndexMetadata(index: Index): Metadata = {
    // build one metadata to hold all index metadata
    val indexMeta = new sql.types.MetadataBuilder()
      .putString("name", index.getName)
      .putStringArray("cols", index.getCols.toArray)
    // adding params if necessary
    if (index.getParams.nonEmpty) {
      val params = new sql.types.MetadataBuilder()
      index.getParams.foreach {
        case (key: String, value: String) => params.putString(key, value)
      }
      indexMeta.putMetadata("params", params.build())
    }
    // Add Key Metadata if defined
    if (index.isEncrypted()) {
      indexMeta.putString("key_metadata", index.getKeyMetadata().get)
    }

    // add the metadata under the index key
    new sql.types.MetadataBuilder()
      .putMetadata("index", indexMeta.build())
      .build()
  }

  /**
    * Generates a map where each index is mapped to all the column paths
    * in the parquet schema generated for it. this is necessary for non-primitive
    * column types such as the ones used for UDTs.
    * IMPORTANT - This function assumes that no index in the input seq maps to a column
    * name that is a prefix of another index's mapped column name.
    *
    * @param indexes   seq of input indexes
    * @param converter implicit [[SparkToParquetSchemaConverter]], else a fresh one is used
    * @return
    */
  private[parquet] def getColumnPathsPerIndex(indexes: Seq[Index])
                                             (implicit converter: SparkToParquetSchemaConverter =
                                             new SparkToParquetSchemaConverter(new SQLConf()))
  : Map[Index, Seq[String]] = {

    // HACK ALERT - passing the tableIdentifier as empty and no key metadata for the footer.
    // this only works since we need the schema tree (names and types) and not metadata, so
    // we can live with the incorrect md returned in the schema
    val parquetSchema = converter.convert(
      createDFSchema(indexes, includeExtraColumns = false, "", None))

    val colPaths = parquetSchema.getPaths().asScala.map(_.mkString("."))
    val map = indexes
      .map(idx => {
        val name = getColumnName(idx)
        (idx, colPaths.filter(_.startsWith(name)))
      })
      .toMap
    map
  }

  /**
    * Generates the param value for the column keys, according to
    * Parquet Modular Encryption's integration with spark and the format
    * used by it.
    * Extracts the key labels and column names from each index and aggregates,
    * grouping all the columns encrypted by the same key together.
    *
    */
  private[parquet] def getColumnKeyListString(indexes: Seq[Index],
                                              footerKeyLabel: String): String = {
    val idxKeysToColumns = getColumnPathsPerIndex(indexes.filter(_.isEncrypted()))
      .toSeq
      .map(tpl => (tpl._1.getKeyMetadata().get, tpl._2.mkString(",")))

    val objNameKey = Seq((footerKeyLabel, "obj_name"))

    val res = (idxKeysToColumns ++ objNameKey)
      .groupBy(_._1)
      .mapValues(_.map { case (a, b) => b })
      .mapValues(_.mkString(","))
      .map { case (a, b) => a + ":" + b }
      .mkString(";")
    res
  }


  private[parquet] def createDFSchema(indexes: Seq[Index],
                                      includeExtraColumns: Boolean = true,
                                      tableIdentifier: String,
                                      footerKey: Option[String],
                                      plainTextFooterEnabled: Boolean = false): StructType = {
    val schemaTranslators = Registration.getCurrentMetaDataTranslators().collect {
      case t: ParquetMetaDataTranslator => t
    }
    val idxFields = indexes.map(idx => StructField(
      getColumnName(idx),
      ParquetUtils.getIndexSchema(idx, schemaTranslators).getOrElse(
        ParquetMetadataStoreUDTRegistrator.getUDTFor(idx.getMetaDataTypeClassName())),
      true,
      createIndexMetadata(idx)))

    val allFields = includeExtraColumns match {
      case true => Seq(StructField("obj_name",
        StringType, false,
        createMasterMetadata(indexes, tableIdentifier, footerKey, plainTextFooterEnabled)
      )) ++ idxFields
      case false => idxFields
    }

    StructType(allFields)
  }


  /**
    * Creates the master metadata (which is assigned to the `obj_name` column)
    * currently contains metadata for encryption (key list and footer key), stored under the
    * `encryption` key.
    * Note some redundancy between the master metadata and the per-index metadata:
    * the per-index metadata contains all the info necessary to generate the column key
    * list - this duplication is to avoid re-creating the list.
    *
    */
  private[parquet] def createMasterMetadata(indexes: Seq[Index],
                                            tableIdentifier: String,
                                            footerKey: Option[String],
                                            plainTextFooterEnabled: Boolean): Metadata = {
    val metaBuilder = new sql.types.MetadataBuilder()
    // version
    metaBuilder.putLong("version", ParquetMetadataStoreConf.PARQUET_MD_STORAGE_VERSION)
    // table Identifier
    metaBuilder.putString("tableIdentifier", tableIdentifier)

    if (indexes.exists(_.isEncrypted())) {
      // this means that at least 1 index is encrypted. verify we have a footer key.
      if (footerKey.isEmpty) {
        throw new ParquetMetaDataStoreException(
          "At least 1 index is marked encrypted but no footer key provided")
      }
      val keysToColumnsString = getColumnKeyListString(indexes, footerKey.get)
      val encryptionMetaBuilder = new sql.types.MetadataBuilder()
      // Column keys
      encryptionMetaBuilder.putString(ParquetMetadataStoreConf.PARQUET_COLUMN_KEYS_SPARK_KEY,
        keysToColumnsString)

      // footer key (always necessary even if plaintext footer is used)
      encryptionMetaBuilder.putString(key = ParquetMetadataStoreConf.PARQUET_FOOTER_KEY_SPARK_KEY,
        value = footerKey.get)
      // plaintext footer mode?
      if (plainTextFooterEnabled) {
        encryptionMetaBuilder.putString(
          key = ParquetMetadataStoreConf.PARQUET_PLAINTEXT_FOOTER_SPARK_KEY, value = "true")
      }

      metaBuilder.putMetadata("encryption", encryptionMetaBuilder.build())
    } else if (footerKey.isDefined) {
      // if no indexes are marked encrypted and the footer key is set, throw an exception
      throw new ParquetMetaDataStoreException("Footer key is set but" +
        " no indexes are marked encrypted")
    }

    metaBuilder.build()
  }

  /**
    * checks if Parquet Modular Encryption (PME) is available
    * the check is performed by verifying that [[org.apache.parquet.crypto.AesCipher]] is available
    * (will be available if and only if PME is loaded)
    *
    * @return true if PME is loaded, else false
    */
  def isPmeAvailable(): Boolean = {
    val clsName = "org.apache.parquet.crypto.AesCipher"
    try {
      val mirror = runtimeMirror(getClass.getClassLoader)
      val module = mirror.staticModule(clsName)
      true
    } catch {
      case _: ScalaReflectionException =>
        false
    }
  }

  /**
    * Given an index and schema translator tries searching for the first available translation.
    * to a native [[DataFrame]] schema. if no translation is found return None
    *
    * @param index       the index to translate
    * @param translators the list of available translators
    * @return the [[DataType]] associated with the translation
    */
  def getIndexSchema(
                      index: Index,
                      translators: Seq[ParquetMetaDataTranslator]): Option[DataType] = {
    // go over the factories and search for the first translation
    var res: Option[DataType] = None
    for (fac <- translators if res == None) {
      res = fac.getDataType(index)
    }
    res
  }


  private[parquet] def isMetadataUpgradePossible(dataFrame: DataFrame): Boolean = {
    val diskVersion = getVersion(dataFrame)
    if (diskVersion == 0) {
      return false
    }
    true
  }


  def mdFileToDF(session: SparkSession, mdPath: String): DataFrame = {
    try {
      session.read.parquet(mdPath)
    } catch {
      case e: SparkException if e.getMessage.contains(
        "is not annotated with SQLUserDefinedType nor registered with UDTRegistration") =>
        throw ParquetMetaDataStoreException(
          "Seems like one of the UDTs is not registered. please make sure all" +
            " UDTs are registered if the problem persists please delete the index" +
            " and try re-indexing", e)
      case e: Exception => throw ParquetMetaDataStoreException(
        s"Metadata file ${mdPath} is corrupted. please delete the index and try re-indexing.", e)
    }
  }

  def getMdVersionStatusFromDf(df: DataFrame): MetadataVersionStatus = {
    getMdVersionStatus(getVersion(df))
  }

  def getMdVersionStatus(version: Long): MetadataVersionStatus = version match {
    case ParquetMetadataStoreConf.PARQUET_MD_STORAGE_VERSION => CURRENT
    case 0 => DEPRECATED_UNSUPPORTED
    case x if x >= ParquetMetadataStoreConf.PARQUET_MINIMUM_SUPPORTED_MD_STORAGE_VERSION =>
      DEPRECATED_SUPPORTED
    case _ => TOO_NEW
  }

  private[parquet] def getTransformedDataFrame(baseDf: DataFrame,
                                               indexes: Seq[Index],
                                               transformMetadata: Boolean = false): DataFrame = {
    val diskVersion = ParquetUtils.getVersion(baseDf)
    val currVersion = ParquetMetadataStoreConf.PARQUET_MD_STORAGE_VERSION
    if (!ParquetUtils.isMetadataUpgradePossible(baseDf)) {
      throw new ParquetMetaDataStoreException(s"cannot upgrade with disk version" +
        s" $diskVersion, current software version $currVersion")
    }

    val indexesCurrColNames = indexes.map(getColumnName(_, currVersion))
    val (indexesMetadata, masterMetadata) = transformMetadata match {
      case true =>
        val newSchema = extractSchema(baseDf.schema, indexes)
        (indexesCurrColNames.map(col => newSchema.apply(col).metadata),
          newSchema.apply("obj_name").metadata)
      case false => (Seq.fill(indexes.size)(Metadata.empty), Metadata.empty)
    }

    val resDf = diskVersion match {
      // rename the columns that were changed
      case x if x == 1L || x == 2L =>
        val objNameUpgradeDescriptor = UpgradeDescriptor("obj_name",
          col("obj_name"),
          "obj_name",
          masterMetadata)
        val upgradeDescriptors =
          Seq(objNameUpgradeDescriptor) ++ indexes.zip(indexesMetadata).map {
            case (idx: Index, md: Metadata) =>
              val origColName = getColumnName(idx, diskVersion)
              val newColName = getColumnName(idx, currVersion)
              val newMetadata = md
              val upgradeExpr = idx match {
                case bf: BloomFilterIndex =>
                  BloomFilterTransformer.transformLegacyBloomFilter(col(origColName))
                case index: Index => col(origColName)
              }

              UpgradeDescriptor(
                origColName,
                upgradeExpr,
                newColName,
                newMetadata)
          }
        upgradeDescriptors.foldLeft(baseDf)(applyUpgradeDescriptor)
      case x if x != currVersion =>
        // this code path can only happen if we change the version and don't address
        // it in this function! it means a match case is semantically missing!!
        val msg = s"asked to upgrade from disk version $diskVersion," +
          s" software version $currVersion but no upgrade path exists!"
        logError(msg)
        throw new ParquetMetaDataStoreException(msg)
      case currVersion => baseDf
    }
    resDf
  }

}
