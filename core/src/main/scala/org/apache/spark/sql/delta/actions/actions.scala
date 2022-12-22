/*
 * Copyright (2021) The Delta Lake Project Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.delta.actions

// scalastyle:off import.ordering.noEmptyLine
import java.net.URI
import java.sql.Timestamp
import java.util.Locale
import java.util.concurrent.TimeUnit

import scala.collection.mutable
import scala.util.control.NonFatal

import org.apache.spark.sql.delta._
import org.apache.spark.sql.delta.constraints.Constraints
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.util.JsonUtils
import com.fasterxml.jackson.annotation._
import com.fasterxml.jackson.annotation.JsonInclude.Include
import com.fasterxml.jackson.core.JsonGenerator
import com.fasterxml.jackson.databind._
import com.fasterxml.jackson.databind.annotation.{JsonDeserialize, JsonSerialize}

import org.apache.spark.internal.Logging
import org.apache.spark.sql.{Column, DataFrame, Dataset, Encoder, SparkSession}
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{DataType, StructField, StructType}
import org.apache.spark.util.Utils

object Action {
  /**
   * The maximum version of the protocol that this version of Delta understands by default.
   *
   * Use [[supportedProtocolVersion()]] instead, except to define new feature-gated versions.
   */
  private[actions] val readerVersion =
    TableFeatureProtocolUtils.TABLE_FEATURES_MIN_READER_VERSION
  private[actions] val writerVersion =
    TableFeatureProtocolUtils.TABLE_FEATURES_MIN_WRITER_VERSION
  private[actions] val protocolVersion: Protocol = Protocol(readerVersion, writerVersion)

  /**
   * The maximum protocol version we are currently allowed to use based on `conf`.
   *
   * This is used to feature-gate certain protocol versions using config values.
   * If no `conf` is given, then this is the same all feature-gated versions being disabled and
   * we return [[protocolVersion]].
   *
   * Feature-gated versions will always be the same or newer than [[protocolVersion]].
   *
   * In all places in the code where we are validating which version can be read, or which version
   * we are currently writing, pass in `conf` so that features are correctly enabled.
   */
  private[delta] def supportedProtocolVersion(
      conf: Option[SQLConf] = None,
      withAllFeatures: Boolean = true): Protocol = {
    val features = if (withAllFeatures) {
      TableFeature.allSupportedFeaturesMap.values
    } else {
      Set.empty
    }
      protocolVersion.withFeatures(features)
  }

  def fromJson(json: String): Action = {
    JsonUtils.mapper.readValue[SingleAction](json).unwrap
  }

  lazy val logSchema = ExpressionEncoder[SingleAction].schema

}

/**
 * Represents a single change to the state of a Delta table. An order sequence
 * of actions can be replayed using [[InMemoryLogReplay]] to derive the state
 * of the table at a given point in time.
 */
sealed trait Action {
  def wrap: SingleAction
  def json: String = JsonUtils.toJson(wrap)
}

/**
 * Used to block older clients from reading or writing the log when backwards incompatible changes
 * are made to the protocol. Readers and writers are responsible for checking that they meet the
 * minimum versions before performing any other operations.
 *
 * This action allows us to explicitly block older clients in the case of a breaking change to the
 * protocol. Absent a protocol change, Clients MUST silently ignore messages and fields that they
 * do not understand.
 *
 * Note: Please initialize this class using the companion object's `apply` method, which will
 * assign correct values (`Set()` vs `None`) to [[readerFeatures]] and [[writerFeatures]].
 */
case class Protocol private (
    minReaderVersion: Int,
    minWriterVersion: Int,
    @JsonInclude(Include.NON_ABSENT) // write to JSON only when the field is not `None`
    readerFeatures: Option[Set[TableFeatureDescriptor]],
    @JsonInclude(Include.NON_ABSENT)
    writerFeatures: Option[Set[TableFeatureDescriptor]])
  extends Action
  with TableFeatureSupport {
  // Correctness check
  // Reader and writer versions must match the status of reader and writer features
  require(
    supportsReaderFeatures == readerFeatures.isDefined,
    "Mismatched minReaderVersion and readerFeatures.")
  require(
    supportsWriterFeatures == writerFeatures.isDefined,
    "Mismatched minWriterVersion and writerFeatures.")

  // When reader is on table features, writer must be on table features too
  if (supportsReaderFeatures && !supportsWriterFeatures) {
    throw DeltaErrors.tableFeatureReadRequiresWriteException(
      TableFeatureProtocolUtils.TABLE_FEATURES_MIN_WRITER_VERSION)
  }

  override def wrap: SingleAction = SingleAction(protocol = this)

  /**
   * Return a reader-friendly string representation of this Protocol.
   *
   * Returns the protocol versions and referenced features when the protocol does support table
   * features, such as `3,7,{},{appendOnly}` and `2,7,None,{appendOnly}`. Otherwise returns only
   * the protocol version such as `2,6`.
   */
  @JsonIgnore
  lazy val simpleString: String = {
    if (!supportsReaderFeatures && !supportsWriterFeatures) {
      s"$minReaderVersion,$minWriterVersion"
    } else {
      val readerFeaturesStr = readerFeatures
        .map(_.map(_.simpleString).toSeq.sorted.mkString("{", ",", "}"))
        .getOrElse("None")
      val writerFeaturesStr = writerFeatures
        .map(_.map(_.simpleString).toSeq.sorted.mkString("{", ",", "}"))
        .getOrElse("None")
      s"$minReaderVersion,$minWriterVersion,$readerFeaturesStr,$writerFeaturesStr"
    }
  }

  override def toString: String = s"Protocol($simpleString)"
}

object Protocol {
  import TableFeatureProtocolUtils._

  val MIN_READER_VERSION_PROP = "delta.minReaderVersion"
  val MIN_WRITER_VERSION_PROP = "delta.minWriterVersion"

  /**
   * Construct a [[Protocol]] case class of the given reader and writer versions. This method will
   * initialize table features fields when reader and writer versions are capable.
   */
  def apply(
      minReaderVersion: Int = Action.readerVersion,
      minWriterVersion: Int = Action.writerVersion): Protocol = {
    new Protocol(
      minReaderVersion = minReaderVersion,
      minWriterVersion = minWriterVersion,
      readerFeatures = if (supportsReaderFeatures(minReaderVersion)) Some(Set()) else None,
      writerFeatures = if (supportsWriterFeatures(minWriterVersion)) Some(Set()) else None)
  }

  /**
   * Given the Delta table metadata, returns the minimum required table protocol that satisfies
   * all active features in the metadata plus protocol-related configs in table properties or
   * session defaults, i.e., configs with keys [[MIN_READER_VERSION_PROP]],
   * [[MIN_WRITER_VERSION_PROP]], [[FEATURE_PROP_PREFIX]], and [[DEFAULT_FEATURE_PROP_PREFIX]].
   */
  def apply(spark: SparkSession, metadataOpt: Option[Metadata]): Protocol = {
    val sessionConf = spark.sessionState.conf
    val tableConf = metadataOpt.map(_.configuration).getOrElse(Map.empty[String, String])

    val minProtocolFromActiveFeaturesOpt =
      metadataOpt.map(m => minProtocolFromActiveFeatures(spark, m))

    // There might be features enabled by the table properties aka
    // `CREATE TABLE ... TBLPROPERTIES ...` or by session defaults.
    val tablePropEnabledFeatures =
      getEnabledFeaturesFromConfigs(tableConf, FEATURE_PROP_PREFIX)
    val sessionEnabledFeatures =
      getEnabledFeaturesFromConfigs(sessionConf.getAllConfs, DEFAULT_FEATURE_PROP_PREFIX)
    val tablePropAndSessionEnabledFeatures = tablePropEnabledFeatures ++ sessionEnabledFeatures

    // Determine the min reader and writer version required by features in table properties or
    // session defaults. If all features are legacy, we start from (0, 0). If any feature is
    // native and reader-writer, we start from (3, 7). Otherwise we start from (0, 7) because
    // there must exist a native writer-only feature.
    val initialProtocol = if (tablePropAndSessionEnabledFeatures.forall(_.isLegacyFeature)) {
      Protocol(0, 0)
    } else if (tablePropAndSessionEnabledFeatures.exists(f =>
        !f.isLegacyFeature && f.isReaderWriterFeature)) {
      Protocol(TABLE_FEATURES_MIN_READER_VERSION, TABLE_FEATURES_MIN_WRITER_VERSION)
    } else {
      Protocol(0, TABLE_FEATURES_MIN_WRITER_VERSION)
    }
    val minProtocolFromFeaturesInTablePropAndSession = initialProtocol.merge(
      tablePropAndSessionEnabledFeatures.map(_.minProtocolVersion).toSeq: _*)

    // If the protocol version is provided in table properties, they will take priority over
    // versions in `minProtocolFromTablePropAndSession`.
    val readerVersionFromTableConfOpt = tableConf
      .get(MIN_READER_VERSION_PROP)
      .map(getVersion(MIN_READER_VERSION_PROP, _))
    val writerVersionFromTableConfOpt = tableConf
      .get(MIN_WRITER_VERSION_PROP)
      .map(getVersion(MIN_WRITER_VERSION_PROP, _))

    // Priority to decide the final protocol version:
    // 1. protocol version defined in table properties
    // 2. max of:
    //    a. protocol version defined in session defaults
    //    b. min version required by automatically-enabled features
    //    c. min version required by features configured in table properties and session defaults
    val finalReaderVersion = readerVersionFromTableConfOpt.getOrElse {
      val candidates = Seq(
        sessionConf.getConf(DeltaSQLConf.DELTA_PROTOCOL_DEFAULT_READER_VERSION),
        minProtocolFromActiveFeaturesOpt.map(_.minReaderVersion).getOrElse(0),
        minProtocolFromFeaturesInTablePropAndSession.minReaderVersion)
      candidates.max
    }
    val finalWriterVersion = writerVersionFromTableConfOpt.getOrElse {
      val candidates = Seq(
        sessionConf.getConf(DeltaSQLConf.DELTA_PROTOCOL_DEFAULT_WRITER_VERSION),
        minProtocolFromActiveFeaturesOpt.map(_.minWriterVersion).getOrElse(0),
        minProtocolFromFeaturesInTablePropAndSession.minWriterVersion)
      candidates.max
    }
    val minActiveFeatures = minProtocolFromActiveFeaturesOpt
      .map(_.readerAndWriterFeatureDescriptors.flatMap(_.toFeature))
      .getOrElse(Set())

    Protocol(finalReaderVersion, finalWriterVersion)
      .withFeatures(minActiveFeatures)
      .withFeatures(tablePropAndSessionEnabledFeatures)
  }

  /**
   * Picks the protocol version for a new table given potential feature usage. The result
   * satisfies all active features in the metadata and protocol-related configs in table
   * properties or session defaults, i.e., configs with keys [[MIN_READER_VERSION_PROP]],
   * [[MIN_WRITER_VERSION_PROP]], [[FEATURE_PROP_PREFIX]], and [[DEFAULT_FEATURE_PROP_PREFIX]].
   */
  def forNewTable(spark: SparkSession, metadata: Metadata): Protocol = {
    Protocol(spark, Some(metadata))
  }

  /**
   * Given the Delta table metadata, returns the minimum required table protocol that satisfies
   * all active features in the metadata.
   *
   * This method does not process protocol-related configs in table properties or session
   * defaults, i.e., configs with keys [[MIN_READER_VERSION_PROP]], [[MIN_WRITER_VERSION_PROP]],
   * [[FEATURE_PROP_PREFIX]], and [[DEFAULT_FEATURE_PROP_PREFIX]].
   */
  def minProtocolFromActiveFeatures(spark: SparkSession, metadata: Metadata): Protocol = {
    var minimumRequired = {
      var (readerVersion, writerVersion) = (0, 0)
      val enabledFeatures = mutable.Set[TableFeature]()
      TableFeature.allSupportedFeaturesMap.values.foreach {
        case feature: TableFeature with FeatureAutomaticallyEnabledByMetadata =>
          if (feature.metadataRequiresFeatureToBeEnabled(metadata, spark)) {
            readerVersion = math.max(readerVersion, feature.minReaderVersion)
            writerVersion = math.max(writerVersion, feature.minWriterVersion)
            enabledFeatures += feature
          }
        case _ => () // Do nothing. We only care about features that can be activated in metadata.
      }
      Protocol(readerVersion, writerVersion).withFeatures(enabledFeatures)
    }

    if (IdentityColumnsTableFeature.metadataRequiresFeatureToBeEnabled(metadata, spark)) {
      throw DeltaErrors.identityColumnNotSupported()
    }


    minimumRequired
  }

  /** Cast the table property for the protocol version to an integer. */
  def getVersion(key: String, value: String): Int = {
    try value.toInt catch {
      case n: NumberFormatException => throw DeltaErrors.protocolPropNotIntException(key, value)
    }
  }

  /**
   * Verify that the protocol version of the table satisfies the version requirements of all the
   * configurations to be set for the table. Returns the minimum required protocol if not.
   */
  def checkProtocolRequirements(
      spark: SparkSession,
      metadata: Metadata,
      current: Protocol): Option[Protocol] = {
    assert(
      !metadata.configuration.contains(MIN_READER_VERSION_PROP),
      "Should not have the " +
        s"protocol version ($MIN_READER_VERSION_PROP) as part of table properties")
    assert(
      !metadata.configuration.contains(MIN_WRITER_VERSION_PROP),
      "Should not have the " +
        s"protocol version ($MIN_WRITER_VERSION_PROP) as part of table properties")
    assert(
      !metadata.configuration.keys.exists(_.startsWith(FEATURE_PROP_PREFIX)),
      "Should not have " +
        s"table features (starts with '$FEATURE_PROP_PREFIX') as part of table properties")
    val required = minProtocolFromActiveFeatures(spark, metadata)
    if (!required.canUpgradeTo(current)) {
      Some(required.merge(current))
    } else {
      None
    }
  }

}

/**
 * Sets the committed version for a given application. Used to make operations
 * like streaming append idempotent.
 */
case class SetTransaction(
    appId: String,
    version: Long,
    @JsonDeserialize(contentAs = classOf[java.lang.Long])
    lastUpdated: Option[Long]) extends Action {
  override def wrap: SingleAction = SingleAction(txn = this)
}

/** Actions pertaining to the addition and removal of files. */
sealed trait FileAction extends Action {
  val path: String
  val dataChange: Boolean
  @JsonIgnore
  val tags: Map[String, String]
  @JsonIgnore
  lazy val pathAsUri: URI = new URI(path)
  @JsonIgnore
  def numLogicalRecords: Option[Long]
  @JsonIgnore
  val partitionValues: Map[String, String]
  @JsonIgnore
  def getFileSize: Long

  /**
   * Return tag value if tags is not null and the tag present.
   */
  @JsonIgnore
  def getTag(tagName: String): Option[String] = Option(tags).flatMap(_.get(tagName))

}

/**
 * Adds a new file to the table. When multiple [[AddFile]] file actions
 * are seen with the same `path` only the metadata from the last one is
 * kept.
 */
case class AddFile(
    override val path: String,
    @JsonInclude(JsonInclude.Include.ALWAYS)
    partitionValues: Map[String, String],
    size: Long,
    modificationTime: Long,
    override val dataChange: Boolean,
    stats: String = null,
    override val tags: Map[String, String] = null
) extends FileAction {
  require(path.nonEmpty)

  override def wrap: SingleAction = SingleAction(add = this)

  def remove: RemoveFile = removeWithTimestamp()

  def removeWithTimestamp(
      timestamp: Long = System.currentTimeMillis(),
      dataChange: Boolean = true
    ): RemoveFile = {
    var newTags = tags
    // scalastyle:off
    val removedFile = RemoveFile(
      path, Some(timestamp), dataChange,
      extendedFileMetadata = Some(true), partitionValues, Some(size), newTags
    )
    // scalastyle:on
    removedFile
  }

  @JsonIgnore
  lazy val insertionTime: Long = tag(AddFile.Tags.INSERTION_TIME).map(_.toLong)
    // From modification time in milliseconds to microseconds.
    .getOrElse(TimeUnit.MICROSECONDS.convert(modificationTime, TimeUnit.MILLISECONDS))


  def tag(tag: AddFile.Tags.KeyType): Option[String] = getTag(tag.name)

  def copyWithTag(tag: AddFile.Tags.KeyType, value: String): AddFile =
    copy(tags = Option(tags).getOrElse(Map.empty) + (tag.name -> value))

  def copyWithoutTag(tag: AddFile.Tags.KeyType): AddFile = {
    if (tags == null) {
      this
    } else {
      copy(tags = tags - tag.name)
    }
  }

  @JsonIgnore
  override def getFileSize: Long = size

  private case class ParsedStatsFields(
      numLogicalRecords: Option[Long]
  )

  @JsonIgnore
  @transient
  private lazy val parsedStatsFields: Option[ParsedStatsFields] = {
    if (stats == null || stats.isEmpty) {
      None
    } else {
      val node = new ObjectMapper().readTree(stats)

      val numLogicalRecords = if (node.has("numRecords")) {
        Some(node.get("numRecords")).filterNot(_.isNull).map(_.asLong())
      } else None

      Some(ParsedStatsFields(
        numLogicalRecords
      ))
    }
  }

  @JsonIgnore
  @transient
  override lazy val numLogicalRecords: Option[Long] =
    parsedStatsFields.flatMap(_.numLogicalRecords)

}

object AddFile {
  /**
   * Misc file-level metadata.
   *
   * The convention is that clients may safely ignore any/all of these tags and this should never
   * have an impact on correctness.
   *
   * Otherwise, the information should go as a field of the AddFile action itself and the Delta
   * protocol version should be bumped.
   */
  object Tags {
    sealed abstract class KeyType(val name: String)

    /** [[ZCUBE_ID]]: identifier of the OPTIMIZE ZORDER BY job that this file was produced by */
    object ZCUBE_ID extends AddFile.Tags.KeyType("ZCUBE_ID")

    /** [[ZCUBE_ZORDER_BY]]: ZOrdering of the corresponding ZCube */
    object ZCUBE_ZORDER_BY extends AddFile.Tags.KeyType("ZCUBE_ZORDER_BY")

    /** [[ZCUBE_ZORDER_CURVE]]: Clustering strategy of the corresponding ZCube */
    object ZCUBE_ZORDER_CURVE extends AddFile.Tags.KeyType("ZCUBE_ZORDER_CURVE")

    /**
     * [[INSERTION_TIME]]: the latest timestamp in micro seconds when the data in the file
     * was inserted
     */
    object INSERTION_TIME extends AddFile.Tags.KeyType("INSERTION_TIME")


    /** [[PARTITION_ID]]: rdd partition id that has written the file, will not be stored in the
     physical log, only used for communication  */
    object PARTITION_ID extends AddFile.Tags.KeyType("PARTITION_ID")

    /** [[OPTIMIZE_TARGET_SIZE]]: target file size the file was optimized to. */
    object OPTIMIZE_TARGET_SIZE extends AddFile.Tags.KeyType("OPTIMIZE_TARGET_SIZE")

  }

  /** Convert a [[Tags.KeyType]] to a string to be used in the AddMap.tags Map[String, String]. */
  def tag(tagKey: Tags.KeyType): String = tagKey.name
}

/**
 * Logical removal of a given file from the reservoir. Acts as a tombstone before a file is
 * deleted permanently.
 *
 * Note that for protocol compatibility reasons, the fields `partitionValues`, `size`, and `tags`
 * are only present when the extendedFileMetadata flag is true. New writers should generally be
 * setting this flag, but old writers (and FSCK) won't, so readers must check this flag before
 * attempting to consume those values.
 *
 * Since old tables would not have `extendedFileMetadata` and `size` field, we should make them
 * nullable by setting their type Option.
 */
// scalastyle:off
case class RemoveFile(
    override val path: String,
    @JsonDeserialize(contentAs = classOf[java.lang.Long])
    deletionTimestamp: Option[Long],
    override val dataChange: Boolean = true,
    extendedFileMetadata: Option[Boolean] = None,
    partitionValues: Map[String, String] = null,
    @JsonDeserialize(contentAs = classOf[java.lang.Long])
    size: Option[Long] = None,
    override val tags: Map[String, String] = null
) extends FileAction {
  override def wrap: SingleAction = SingleAction(remove = this)

  @JsonIgnore
  val delTimestamp: Long = deletionTimestamp.getOrElse(0L)

  /** The number of records contained inside the removed file. */
  @JsonIgnore
  var numLogicalRecords: Option[Long] = None


  /**
   * Create a copy with the new tag. `extendedFileMetadata` is copied unchanged.
   */
  def copyWithTag(tag: String, value: String): RemoveFile = copy(
    tags = Option(tags).getOrElse(Map.empty) + (tag -> value))

  /**
   * Create a copy without the tag.
   */
  def copyWithoutTag(tag: String): RemoveFile =
    copy(tags = Option(tags).getOrElse(Map.empty) - tag)

  @JsonIgnore
  override def getFileSize: Long = size.getOrElse(0L)

}
// scalastyle:on

/**
 * A change file containing CDC data for the Delta version it's within. Non-CDC readers should
 * ignore this, CDC readers should scan all ChangeFiles in a version rather than computing
 * changes from AddFile and RemoveFile actions.
 */
case class AddCDCFile(
    override val path: String,
    @JsonInclude(JsonInclude.Include.ALWAYS)
    partitionValues: Map[String, String],
    size: Long,
    override val tags: Map[String, String] = null) extends FileAction {
  override val dataChange = false

  override def wrap: SingleAction = SingleAction(cdc = this)

  @JsonIgnore
  override def getFileSize: Long = size

  @JsonIgnore
  override def numLogicalRecords: Option[Long] = None
}


case class Format(
    provider: String = "parquet",
    // If we support `options` in future, we should not store any file system options since they may
    // contain credentials.
    options: Map[String, String] = Map.empty)

/**
 * Updates the metadata of the table. Only the last update to the [[Metadata]]
 * of a table is kept. It is the responsibility of the writer to ensure that
 * any data already present in the table is still valid after any change.
 */
case class Metadata(
    id: String = if (Utils.isTesting) "testId" else java.util.UUID.randomUUID().toString,
    name: String = null,
    description: String = null,
    format: Format = Format(),
    schemaString: String = null,
    partitionColumns: Seq[String] = Nil,
    configuration: Map[String, String] = Map.empty,
    @JsonDeserialize(contentAs = classOf[java.lang.Long])
    createdTime: Option[Long] = None) extends Action {

  // The `schema` and `partitionSchema` methods should be vals or lazy vals, NOT
  // defs, because parsing StructTypes from JSON is extremely expensive and has
  // caused perf. problems here in the past:

  /**
   * Column mapping mode for this table
   */
  @JsonIgnore
  lazy val columnMappingMode: DeltaColumnMappingMode =
    DeltaConfigs.COLUMN_MAPPING_MODE.fromMetaData(this)

  /**
   * Column mapping max id for this table
   */
  @JsonIgnore
  lazy val columnMappingMaxId: Long =
    DeltaConfigs.COLUMN_MAPPING_MAX_ID.fromMetaData(this)

  /** Returns the schema as a [[StructType]] */
  @JsonIgnore
  lazy val schema: StructType = Option(schemaString)
    .map(DataType.fromJson(_).asInstanceOf[StructType])
    .getOrElse(StructType.apply(Nil))

  /** Returns the partitionSchema as a [[StructType]] */
  @JsonIgnore
  lazy val partitionSchema: StructType =
    new StructType(partitionColumns.map(c => schema(c)).toArray)

  /** Partition value keys in the AddFile map. */
  @JsonIgnore
  lazy val physicalPartitionSchema: StructType = DeltaColumnMapping.renameColumns(partitionSchema)

  /** Columns written out to files. */
  @JsonIgnore
  lazy val dataSchema: StructType = {
    val partitions = partitionColumns.toSet
    StructType(schema.filterNot(f => partitions.contains(f.name)))
  }

  /** Partition value written out to files */
  @JsonIgnore
  lazy val physicalPartitionColumns: Seq[String] = physicalPartitionSchema.fieldNames.toSeq

  /**
   * Columns whose type should never be changed. For example, if a column is used by a generated
   * column, changing its type may break the constraint defined by the generation expression. Hence,
   * we should never change its type.
   */
  @JsonIgnore
  lazy val fixedTypeColumns: Set[String] =
    GeneratedColumn.getGeneratedColumnsAndColumnsUsedByGeneratedColumns(schema)

  /**
   * Store non-partition columns and their corresponding [[OptimizablePartitionExpression]] which
   * can be used to create partition filters from data filters of these non-partition columns.
   */
  @JsonIgnore
  lazy val optimizablePartitionExpressions: Map[String, Seq[OptimizablePartitionExpression]]
  = GeneratedColumn.getOptimizablePartitionExpressions(schema, partitionSchema)

  override def wrap: SingleAction = SingleAction(metaData = this)
}

/**
 * Interface for objects that represents the information for a commit. Commits can be referred to
 * using a version and timestamp. The timestamp of a commit comes from the remote storage
 * `lastModifiedTime`, and can be adjusted for clock skew. Hence we have the method `withTimestamp`.
 */
trait CommitMarker {
  /** Get the timestamp of the commit as millis after the epoch. */
  def getTimestamp: Long
  /** Return a copy object of this object with the given timestamp. */
  def withTimestamp(timestamp: Long): CommitMarker
  /** Get the version of the commit. */
  def getVersion: Long
}

/**
 * Holds provenance information about changes to the table. This [[Action]]
 * is not stored in the checkpoint and has reduced compatibility guarantees.
 * Information stored in it is best effort (i.e. can be falsified by the writer).
 *
 * @param isBlindAppend Whether this commit has blindly appended without caring about existing files
 * @param engineInfo The information for the engine that makes the commit.
 *                   If a commit is made by Delta Lake 1.1.0 or above, it will be
 *                   `Apache-Spark/x.y.z Delta-Lake/x.y.z`.
 */
case class CommitInfo(
    // The commit version should be left unfilled during commit(). When reading a delta file, we can
    // infer the commit version from the file name and fill in this field then.
    @JsonDeserialize(contentAs = classOf[java.lang.Long])
    version: Option[Long],
    timestamp: Timestamp,
    userId: Option[String],
    userName: Option[String],
    operation: String,
    @JsonSerialize(using = classOf[JsonMapSerializer])
    operationParameters: Map[String, String],
    job: Option[JobInfo],
    notebook: Option[NotebookInfo],
    clusterId: Option[String],
    @JsonDeserialize(contentAs = classOf[java.lang.Long])
    readVersion: Option[Long],
    isolationLevel: Option[String],
    isBlindAppend: Option[Boolean],
    operationMetrics: Option[Map[String, String]],
    userMetadata: Option[String],
    tags: Option[Map[String, String]],
    engineInfo: Option[String],
    txnId: Option[String]) extends Action with CommitMarker {
  override def wrap: SingleAction = SingleAction(commitInfo = this)

  override def withTimestamp(timestamp: Long): CommitInfo = {
    this.copy(timestamp = new Timestamp(timestamp))
  }

  override def getTimestamp: Long = timestamp.getTime
  @JsonIgnore
  override def getVersion: Long = version.get

}

case class JobInfo(
    jobId: String,
    jobName: String,
    runId: String,
    jobOwnerId: String,
    triggerType: String)

object JobInfo {
  def fromContext(context: Map[String, String]): Option[JobInfo] = {
    context.get("jobId").map { jobId =>
      JobInfo(
        jobId,
        context.get("jobName").orNull,
        context.get("runId").orNull,
        context.get("jobOwnerId").orNull,
        context.get("jobTriggerType").orNull)
    }
  }
}

case class NotebookInfo(notebookId: String)

object NotebookInfo {
  def fromContext(context: Map[String, String]): Option[NotebookInfo] = {
    context.get("notebookId").map { nbId => NotebookInfo(nbId) }
  }
}

object CommitInfo {
  def empty(version: Option[Long] = None): CommitInfo = {
    CommitInfo(version, null, None, None, null, null, None, None,
      None, None, None, None, None, None, None, None, None)
  }

  // scalastyle:off argcount
  def apply(
      time: Long,
      operation: String,
      operationParameters: Map[String, String],
      commandContext: Map[String, String],
      readVersion: Option[Long],
      isolationLevel: Option[String],
      isBlindAppend: Option[Boolean],
      operationMetrics: Option[Map[String, String]],
      userMetadata: Option[String],
      tags: Option[Map[String, String]],
      txnId: Option[String]): CommitInfo = {

    val getUserName = commandContext.get("user").flatMap {
      case "unknown" => None
      case other => Option(other)
    }

    CommitInfo(
      None,
      new Timestamp(time),
      commandContext.get("userId"),
      getUserName,
      operation,
      operationParameters,
      JobInfo.fromContext(commandContext),
      NotebookInfo.fromContext(commandContext),
      commandContext.get("clusterId"),
      readVersion,
      isolationLevel,
      isBlindAppend,
      operationMetrics,
      userMetadata,
      tags,
      getEngineInfo,
      txnId)
  }
  // scalastyle:on argcount

  private def getEngineInfo: Option[String] = {
    Some(s"Apache-Spark/${org.apache.spark.SPARK_VERSION} Delta-Lake/${io.delta.VERSION}")
  }

}

/** A serialization helper to create a common action envelope. */
case class SingleAction(
    txn: SetTransaction = null,
    add: AddFile = null,
    remove: RemoveFile = null,
    metaData: Metadata = null,
    protocol: Protocol = null,
    cdc: AddCDCFile = null,
    commitInfo: CommitInfo = null) {

  def unwrap: Action = {
    if (add != null) {
      add
    } else if (remove != null) {
      remove
    } else if (metaData != null) {
      metaData
    } else if (txn != null) {
      txn
    } else if (protocol != null) {
      protocol
    } else if (cdc != null) {
      cdc
    } else if (commitInfo != null) {
      commitInfo
    } else {
      null
    }
  }
}

object SingleAction extends Logging {
  implicit def encoder: Encoder[SingleAction] =
    org.apache.spark.sql.delta.implicits.singleActionEncoder

  implicit def addFileEncoder: Encoder[AddFile] =
    org.apache.spark.sql.delta.implicits.addFileEncoder

  lazy val nullLitForRemoveFile: Column =
    new Column(Literal(null, ScalaReflection.schemaFor[RemoveFile].dataType))

  lazy val nullLitForAddCDCFile: Column =
    new Column(Literal(null, ScalaReflection.schemaFor[AddCDCFile].dataType))
}

/** Serializes Maps containing JSON strings without extra escaping. */
class JsonMapSerializer extends JsonSerializer[Map[String, String]] {
  def serialize(
      parameters: Map[String, String],
      jgen: JsonGenerator,
      provider: SerializerProvider): Unit = {
    jgen.writeStartObject()
    parameters.foreach { case (key, value) =>
      if (value == null) {
        jgen.writeNullField(key)
      } else {
        jgen.writeFieldName(key)
        // Write value as raw data, since it's already JSON text
        jgen.writeRawValue(value)
      }
    }
    jgen.writeEndObject()
  }
}
