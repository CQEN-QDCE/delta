/*
 * Copyright 2019 Databricks, Inc.
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

package org.apache.spark.sql.delta.catalog

import java.util

import scala.collection.JavaConverters._
import scala.collection.mutable

import org.apache.spark.sql.connector.catalog.{SupportsWrite, Table, TableCapability}
import org.apache.spark.sql.connector.catalog.TableCapability._
import org.apache.spark.sql.connector.expressions.{FieldReference, IdentityTransform, Transform}
import org.apache.spark.sql.connector.write.{SupportsOverwrite, SupportsTruncate, V1WriteBuilder, WriteBuilder}
import org.apache.spark.sql.{AnalysisException, DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.delta.{DeltaLog, DeltaOptions}
import org.apache.spark.sql.delta.commands.WriteIntoDelta
import org.apache.spark.sql.delta.sources.{DeltaDataSource, DeltaSourceUtils}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.sources
import org.apache.spark.sql.sources.{Filter, InsertableRelation}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

/**
 * The data source V2 representation of a Delta table that exists.
 *
 * @param log The Delta log.
 * @param specifiedSchema The user-specified schema for this table instance.
 * @param specifiedPartitioning The user-specified partitioning for this table instance.
 */
case class DeltaTableV2(
    log: DeltaLog,
    specifiedSchema: Option[StructType] = None,
    specifiedPartitioning: Array[Transform] = Array.empty,
    tableIdentifier: Option[String] = None) extends Table with SupportsWrite {
  override def name(): String = tableIdentifier.getOrElse(s"delta.`${log.dataPath}`")
  override def schema(): StructType = log.snapshot.schema

  override def partitioning(): Array[Transform] = {
    log.snapshot.metadata.partitionColumns.map { col =>
      IdentityTransform(FieldReference(Seq(col)))
    }.toArray
  }

  override def capabilities(): util.Set[TableCapability] = Set(
    ACCEPT_ANY_SCHEMA, BATCH_READ,
    V1_BATCH_WRITE, OVERWRITE_BY_FILTER, TRUNCATE
  ).asJava

  override def newWriteBuilder(options: CaseInsensitiveStringMap): WriteBuilder = {
    new WriteIntoDeltaBuilder(log, options, specifiedSchema, specifiedPartitioning)
  }
}

private class WriteIntoDeltaBuilder(
    log: DeltaLog,
    writeOptions: CaseInsensitiveStringMap,
    specifiedSchema: Option[StructType] = None,
    specifiedPartitioning: Array[Transform] = Array.empty)
  extends WriteBuilder with V1WriteBuilder with SupportsOverwrite with SupportsTruncate {

  private var forceOverwrite = false

  private val options =
    mutable.HashMap[String, String](writeOptions.asCaseSensitiveMap().asScala.toSeq: _*)

  override def truncate(): WriteIntoDeltaBuilder = {
    forceOverwrite = true
    this
  }

  override def overwrite(filters: Array[Filter]): WriteBuilder = {
    if (writeOptions.containsKey("replaceWhere")) {
      throw new AnalysisException(
        "You can't use replaceWhere in conjunction with an overwrite by filter")
    }
    options.put("replaceWhere", DeltaSourceUtils.translateFilters(filters).sql)
    forceOverwrite = true
    this
  }

  override def buildForV1Write(): InsertableRelation = {
    new InsertableRelation {
      override def insert(data: DataFrame, overwrite: Boolean): Unit = {
        val specifiedPartitionColumns = specifiedPartitioning.map {
          case IdentityTransform(FieldReference(nameParts)) =>
            if (nameParts.size != 1) {
              throw new IllegalArgumentException(s"Invalid nested partition column $nameParts")
            }
            nameParts.head
          case transform =>
            throw new IllegalArgumentException(s"Unsupported partition transform $transform")
        }
        WriteIntoDelta(
          log,
          // TODO: other save modes?
          if (forceOverwrite) SaveMode.Overwrite else SaveMode.Append,
          new DeltaOptions(options.toMap, SQLConf.get),
          specifiedPartitionColumns,
          log.snapshot.metadata.configuration,
          data).run(SparkSession.active)
      }
    }
  }
}
