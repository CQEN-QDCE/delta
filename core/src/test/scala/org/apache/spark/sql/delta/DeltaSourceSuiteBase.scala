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

package org.apache.spark.sql.delta

import java.io.File

import org.apache.spark.sql.delta.actions.Format
import org.apache.spark.sql.delta.schema.{SchemaMergingUtils, SchemaUtils}

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.streaming.StreamTest
import org.apache.spark.sql.types.StructType

trait DeltaSourceSuiteBase extends StreamTest {

  /**
   * Copy metadata for fields in newSchema from currentSchema
   * @param newSchema new schema
   * @param currentSchema current schema to reference
   * @param columnMappingMode mode for column mapping
   * @return updated new schema
   */
  protected def copyOverMetadata(
      newSchema: StructType,
      currentSchema: StructType,
      columnMappingMode: DeltaColumnMappingMode): StructType = {
    SchemaMergingUtils.transformColumns(newSchema) { (path, field, _) =>
      val fullName = path :+ field.name
      val inSchema = SchemaUtils.findNestedFieldIgnoreCase(
        currentSchema, fullName, includeCollections = true
      )
      inSchema.map { refField =>
        val sparkMetadata = DeltaColumnMapping.getColumnMappingMetadata(refField, columnMappingMode)
        field.copy(metadata = sparkMetadata)
      }.getOrElse {
        field
      }
    }
  }

  protected def withMetadata(
      deltaLog: DeltaLog,
      schema: StructType,
      format: String = "parquet",
      tableId: Option[String] = None): Unit = {
    val txn = deltaLog.startTransaction()
    val baseMetadata = tableId.map { tId => txn.metadata.copy(id = tId) }.getOrElse(txn.metadata)
    // We need to fill up the missing id/physical name in column mapping mode
    // while maintaining existing metadata if there is any
    val updatedMetadata = copyOverMetadata(
      schema, baseMetadata.schema,
      baseMetadata.columnMappingMode)
    txn.commit(
      DeltaColumnMapping.assignColumnIdAndPhysicalName(
        baseMetadata.copy(
          schemaString = updatedMetadata.json,
          format = Format(format)),
        baseMetadata,
        false) :: Nil, DeltaOperations.ManualUpdate)
  }

  object AddToReservoir {
    def apply(path: File, data: DataFrame): AssertOnQuery =
      AssertOnQuery { _ =>
        data.write.format("delta").mode("append").save(path.getAbsolutePath)
        true
      }
  }

  object CheckProgress {
    def apply(rowsPerBatch: Seq[Int]): AssertOnQuery =
      Execute { q =>
        val progress = q.recentProgress.filter(_.numInputRows != 0)
        assert(progress.length === rowsPerBatch.size, "Expected batches don't match")
        progress.zipWithIndex.foreach { case (p, i) =>
          assert(p.numInputRows === rowsPerBatch(i), s"Expected rows in batch $i does not match ")
        }
      }
  }
}
