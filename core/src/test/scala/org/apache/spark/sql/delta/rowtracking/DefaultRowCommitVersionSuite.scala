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

package org.apache.spark.sql.delta.rowtracking

import scala.collection.mutable

import org.apache.spark.sql.delta.DeltaLog
import org.apache.spark.sql.delta.actions.{AddFile, RemoveFile}
import org.apache.spark.sql.delta.rowid.RowIdTestUtils
import org.apache.spark.sql.delta.stats.DeltaScan

import org.apache.spark.SparkConf
import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.test.SharedSparkSession

class DefaultRowCommitVersionSuite extends QueryTest with SharedSparkSession with RowIdTestUtils {

  def expectedCommitVersionsForAllFiles(deltaLog: DeltaLog): Map[String, Long] = {
    val commitVersionForFiles = mutable.Map.empty[String, Long]
    deltaLog.getChanges(startVersion = 0).foreach { case (commitVersion, actions) =>
      actions.foreach {
        case a: AddFile if !commitVersionForFiles.contains(a.path) =>
          commitVersionForFiles += a.path -> commitVersion
        case r: RemoveFile if commitVersionForFiles.contains(r.path) =>
          assert(r.defaultRowCommitVersion.contains(commitVersionForFiles(r.path)))
        case _ =>
          // Do nothing
      }
    }
    commitVersionForFiles.toMap
  }

  test("defaultRowCommitVersion is not set when feature is disabled") {
    withRowTrackingEnabled(enabled = false) {
      withTempDir { tempDir =>
        spark.range(start = 0, end = 100, step = 1, numPartitions = 1)
          .write.format("delta").mode("overwrite").save(tempDir.getAbsolutePath)
        spark.range(start = 100, end = 200, step = 1, numPartitions = 1)
          .write.format("delta").mode("append").save(tempDir.getAbsolutePath)

        val deltaLog = DeltaLog.forTable(spark, tempDir)
        deltaLog.update().allFiles.collect().foreach { f =>
          assert(f.defaultRowCommitVersion.isEmpty)
        }
      }
    }
  }

  test("checkpoint preserves defaultRowCommitVersion") {
    withRowTrackingEnabled(enabled = true) {
      withTempDir { tempDir =>
        spark.range(start = 0, end = 100, step = 1, numPartitions = 1)
          .write.format("delta").mode("append").save(tempDir.getAbsolutePath)
        spark.range(start = 100, end = 200, step = 1, numPartitions = 1)
          .write.format("delta").mode("append").save(tempDir.getAbsolutePath)
        spark.range(start = 200, end = 300, step = 1, numPartitions = 1)
          .write.format("delta").mode("append").save(tempDir.getAbsolutePath)

        val deltaLog = DeltaLog.forTable(spark, tempDir)
        val commitVersionForFiles = expectedCommitVersionsForAllFiles(deltaLog)

        deltaLog.update().allFiles.collect().foreach { f =>
          assert(f.defaultRowCommitVersion.contains(commitVersionForFiles(f.path)))
        }

        deltaLog.checkpoint(deltaLog.update())

        deltaLog.update().allFiles.collect().foreach { f =>
          assert(f.defaultRowCommitVersion.contains(commitVersionForFiles(f.path)))
        }
      }
    }
  }

  test("data skipping reads defaultRowCommitVersion") {
    withRowTrackingEnabled(enabled = true) {
      withTempDir { tempDir =>
        spark.range(start = 0, end = 100, step = 1, numPartitions = 1)
          .write.format("delta").mode("append").save(tempDir.getAbsolutePath)
        spark.range(start = 100, end = 200, step = 1, numPartitions = 1)
          .write.format("delta").mode("append").save(tempDir.getAbsolutePath)
        spark.range(start = 200, end = 300, step = 1, numPartitions = 1)
          .write.format("delta").mode("append").save(tempDir.getAbsolutePath)

        val deltaLog = DeltaLog.forTable(spark, tempDir)
        val commitVersionForFiles = expectedCommitVersionsForAllFiles(deltaLog)

        val filters: Seq[Expression] = Seq(col("id = 150").expr)
        val scan: DeltaScan = deltaLog.update().filesForScan(filters)

        scan.files.foreach { f =>
          assert(f.defaultRowCommitVersion.contains(commitVersionForFiles(f.path)))
        }
      }
    }
  }

  test("clone does not preserve default row commit versions") {
    withRowTrackingEnabled(enabled = true) {
      withTempDir { sourceDir =>
        spark.range(start = 0, end = 100, step = 1, numPartitions = 1)
          .write.format("delta").mode("append").save(sourceDir.getAbsolutePath)
        spark.range(start = 100, end = 200, step = 1, numPartitions = 1)
          .write.format("delta").mode("append").save(sourceDir.getAbsolutePath)
        spark.range(start = 200, end = 300, step = 1, numPartitions = 1)
          .write.format("delta").mode("append").save(sourceDir.getAbsolutePath)

        withTable("target") {
          spark.sql(s"CREATE TABLE target SHALLOW CLONE delta.`${sourceDir.getAbsolutePath}`")

          val targetLog = DeltaLog.forTable(spark, TableIdentifier("target"))
          targetLog.update().allFiles.collect().foreach { f =>
            assert(f.defaultRowCommitVersion.contains(0L))
          }
        }
      }
    }
  }

  test("restore does preserve default row commit versions") {
    withRowTrackingEnabled(enabled = true) {
      withTempDir { tempDir =>
        spark.range(start = 0, end = 100, step = 1, numPartitions = 1)
          .write.format("delta").mode("append").save(tempDir.getAbsolutePath)
        spark.range(start = 100, end = 200, step = 1, numPartitions = 1)
          .write.format("delta").mode("append").save(tempDir.getAbsolutePath)
        spark.range(start = 200, end = 300, step = 1, numPartitions = 1)
          .write.format("delta").mode("append").save(tempDir.getAbsolutePath)

        val deltaLog = DeltaLog.forTable(spark, tempDir)
        val commitVersionForFiles = expectedCommitVersionsForAllFiles(deltaLog)

        spark.sql(s"RESTORE delta.`${tempDir.getAbsolutePath}` TO VERSION AS OF 1")

        deltaLog.update().allFiles.collect().foreach { f =>
          assert(f.defaultRowCommitVersion.contains(commitVersionForFiles(f.path)))
        }
      }
    }
  }
}
