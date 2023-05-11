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

package org.apache.spark.sql.delta.rowid

import org.apache.spark.sql.delta.{DeltaConfigs, DeltaIllegalStateException, DeltaLog, RowId}
import org.apache.spark.sql.delta.DeltaOperations.ManualUpdate
import org.apache.spark.sql.delta.actions.RowIdHighWaterMark
import org.apache.spark.sql.delta.actions.TableFeatureProtocolUtils.TABLE_FEATURES_MIN_WRITER_VERSION
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.util.FileNames

import org.apache.spark.sql.{QueryTest, Row}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.test.SharedSparkSession

class RowIdSuite extends QueryTest
    with SharedSparkSession
    with RowIdTestUtils {
  test("Creating a new table with row ID table feature sets row IDs as readable") {
    withRowIdsEnabled(enabled = false) {
      withTable("tbl") {
        spark.range(10).write.format("delta")
          .option(rowIdFeatureName, "supported").saveAsTable("tbl")

        val log = DeltaLog.forTable(spark, TableIdentifier("tbl"))
        assert(RowId.rowIdsEnabled(log.update().protocol, log.update().metadata))
      }
    }
  }

  test("Enabling row IDs on existing table does not set row IDs as readable") {
    withRowIdsEnabled(enabled = false) {
      withTable("tbl") {
        spark.range(10).write.format("delta")
          .saveAsTable("tbl")

        sql(
          s"""
             |ALTER TABLE tbl
             |SET TBLPROPERTIES (
             |'$rowIdFeatureName' = 'supported',
             |'delta.minWriterVersion' = $TABLE_FEATURES_MIN_WRITER_VERSION)""".stripMargin)

        val log = DeltaLog.forTable(spark, TableIdentifier("tbl"))
        assert(RowId.rowIdsSupported(log.update().protocol))
        assert(!RowId.rowIdsEnabled(log.update().protocol, log.update().metadata))
      }
    }
  }

  test("row ids are assigned when they are enabled") {
    withRowIdsEnabled(enabled = true) {
      withTempDir { dir =>
        spark.range(start = 0, end = 1000, step = 1, numPartitions = 10)
          .write.format("delta").save(dir.getAbsolutePath)
        val log = DeltaLog.forTable(spark, dir)
        assertRowIdsAreValid(log)

        spark.range(start = 1000, end = 1500, step = 1, numPartitions = 3)
          .write.format("delta").mode("append").save(dir.getAbsolutePath)
        assertRowIdsAreValid(log)
      }
    }
  }

  test("row ids are not assigned when they are disabled") {
    withRowIdsEnabled(enabled = false) {
      withTempDir { dir =>
        spark.range(start = 0, end = 1000, step = 1, numPartitions = 10)
          .write.format("delta").save(dir.getAbsolutePath)
        val log = DeltaLog.forTable(spark, dir)
        assertRowIdsAreNotSet(log)

        spark.range(start = 1000, end = 1500, step = 1, numPartitions = 3)
          .write.format("delta").mode("append").save(dir.getAbsolutePath)
        assertRowIdsAreNotSet(log)
      }
    }
  }

  test("row ids are not assigned when they are not allowed") {
    withRowIdsEnabled(enabled = true) {
      // Setting the table property, but disabling via config.
      withSQLConf(DeltaSQLConf.ROW_IDS_ALLOWED.key -> "false") {
        withTempDir { dir =>
          spark.range(start = 0, end = 1000, step = 1, numPartitions = 10)
            .write.format("delta").save(dir.getAbsolutePath)
          val log = DeltaLog.forTable(spark, dir)
          assertRowIdsAreNotSet(log)

          spark.range(start = 1000, end = 1500, step = 1, numPartitions = 3)
            .write.format("delta").mode("append").save(dir.getAbsolutePath)
          assertRowIdsAreNotSet(log)
        }
      }
    }
  }

  test("row ids can be disabled") {
    withRowIdsEnabled(enabled = true) {
      withTempDir { dir =>
        spark.range(start = 0, end = 1000, step = 1, numPartitions = 10)
          .write.format("delta").save(dir.getAbsolutePath)
        val log = DeltaLog.forTable(spark, dir)
        assertRowIdsAreValid(log)

        sql(s"ALTER TABLE delta.`${dir.getAbsolutePath}` " +
          s"SET TBLPROPERTIES ('${DeltaConfigs.ROW_IDS_ENABLED.key}' = false)")
        checkAnswer(
          spark.read.load(dir.getAbsolutePath),
          (0 until 1000).map(Row(_)))
      }
    }
  }

  test("high watermark survives checkpointing") {
    withRowIdsEnabled(enabled = true) {
      withTempDir { dir =>
        spark.range(start = 0, end = 1000, step = 1, numPartitions = 10)
          .write.format("delta").save(dir.getAbsolutePath)
        val log1 = DeltaLog.forTable(spark, dir)
        assertRowIdsAreValid(log1)

        // Force a checkpoint and add an empty commit, so that we can delete the first commit
        log1.checkpoint(log1.update())
        log1.startTransaction().commit(Nil, ManualUpdate)
        DeltaLog.clearCache()

        // Delete the first commit and all checksum files to force the next read to read the high
        // watermark from the checkpoint.
        val fs = log1.logPath.getFileSystem(log1.newDeltaHadoopConf())
        fs.delete(FileNames.deltaFile(log1.logPath, version = 0), true)
        fs.delete(FileNames.checksumFile(log1.logPath, version = 0), true)
        fs.delete(FileNames.checksumFile(log1.logPath, version = 1), true)

        spark.range(start = 1000, end = 1500, step = 1, numPartitions = 3)
          .write.format("delta").mode("append").save(dir.getAbsolutePath)
        val log2 = DeltaLog.forTable(spark, dir)
        assertRowIdsAreValid(log2)
      }
    }
  }

  test("re-added files keep their row ids") {
    withRowIdsEnabled(enabled = true) {
      withTempDir { dir =>
        spark.range(start = 0, end = 1000, step = 1, numPartitions = 10)
          .write.format("delta").save(dir.getAbsolutePath)
        val log = DeltaLog.forTable(spark, dir)
        assertRowIdsAreValid(log)

        val filesBefore = log.update().allFiles.collect()
        val baseRowIdsBefore = filesBefore.map(f => f.path -> f.baseRowId.get).toMap

        log.startTransaction().commit(filesBefore, ManualUpdate)
        assertRowIdsAreValid(log)

        val filesAfter = log.update().allFiles.collect()
        val baseRowIdsAfter = filesAfter.map(f => f.path -> f.baseRowId.get).toMap

        assert(baseRowIdsBefore == baseRowIdsAfter)
      }
    }
  }

  test("RESTORE retains high watermark") {
    withRowIdsEnabled(enabled = true) {
      withTempDir { dir =>
        // version 0: high watermark = 9
        spark.range(start = 0, end = 10)
          .write.format("delta").save(dir.getAbsolutePath)
        val log = DeltaLog.forTable(spark, dir)
        val deltaTable = io.delta.tables.DeltaTable.forPath(spark, dir.getAbsolutePath)

        // version 1: high watermark = 19
        spark.range(start = 10, end = 20)
          .write.mode("append").format("delta").save(dir.getAbsolutePath)
        val highWatermarkBeforeRestore = RowId.extractHighWatermark(spark, log.update())

        // back to version 0: high watermark should be still equal to before the restore.
        deltaTable.restoreToVersion(0)

        val highWatermarkAfterRestore = RowId.extractHighWatermark(spark, log.update())
        assert(highWatermarkBeforeRestore == highWatermarkAfterRestore)
        assertRowIdsDoNotOverlap(log)

        // version 1 (overridden): high watermark = 29
        spark.range(start = 10, end = 20)
          .write.mode("append").format("delta").save(dir.getAbsolutePath)
        assertHighWatermarkIsCorrectAfterUpdate(
          log,
          highWatermarkBeforeUpdate = highWatermarkAfterRestore.get.highWaterMark,
          expectedNumRecordsWritten = 10)
        assertRowIdsDoNotOverlap(log)
        val highWatermarkWithNewData = RowId.extractHighWatermark(spark, log.update())

        // back to version 0: high watermark should still be 29.
        deltaTable.restoreToVersion(0)

        val highWatermarkWithNewDataAfterRestore =
          RowId.extractHighWatermark(spark, log.update())
        assert(highWatermarkWithNewData == highWatermarkWithNewDataAfterRestore)
        assertRowIdsDoNotOverlap(log)

      }
    }
  }

  test("row_id column with row ids disabled") {
    withRowIdsEnabled(enabled = false) {
      withTempDir { dir =>
        spark.range(start = 0, end = 1000, step = 1, numPartitions = 5)
          .select((col("id") + 10000L).as("row_id"))
          .write.format("delta").save(dir.getAbsolutePath)

        checkAnswer(
          spark.read.load(dir.getAbsolutePath),
          (0 until 1000).map(i => Row(i + 10000L))
        )
      }
    }
  }

  test("Throws error when assigning row IDs without stats") {
    withSQLConf(
      defaultRowIdFeatureProperty -> "supported",
      DeltaSQLConf.DELTA_COLLECT_STATS.key -> "false") {
      withTable("target") {
        val err = intercept[UnsupportedOperationException] {
          spark.range(end = 10).write.format("delta").saveAsTable("target")
        }
        assert(err.getMessage === "Cannot assign row IDs without row count statistics.")
      }
    }
  }

  test("manually setting row ID high watermark is not allowed") {
    withRowIdsEnabled(enabled = true) {
      withTempDir { dir =>
        spark.range(start = 0, end = 1000, step = 1, numPartitions = 10)
          .write.format("delta").save(dir.getAbsolutePath)

        val log = DeltaLog.forTable(spark, dir)

        val exception = intercept[IllegalStateException] {
          log.startTransaction().commit(Seq(RowIdHighWaterMark(highWaterMark = 9001)), ManualUpdate)
        }
        assert(exception.getMessage.contains(
          "Manually setting the Row ID high water mark is not allowed"))
      }
    }
  }

  test("ALTER TABLE cannot enable Row IDs on existing table") {
    withRowIdsEnabled(enabled = false) {
      withTable("tbl") {
        spark.range(10).write.format("delta").saveAsTable("tbl")

        val log = DeltaLog.forTable(spark, TableIdentifier("tbl"))
        assert(!RowId.rowIdsEnabled(log.update().protocol, log.update().metadata))

        val err = intercept[UnsupportedOperationException] {
          sql(s"ALTER TABLE tbl SET TBLPROPERTIES ('${DeltaConfigs.ROW_IDS_ENABLED.key}' = true)")
        }
        assert(err.getMessage === "Cannot enable Row IDs on an existing table.")
      }
    }
  }
}
