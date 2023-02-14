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

package org.apache.spark.sql.delta.deletionvectors

import java.io.File

import org.apache.spark.sql.delta.{DeletionVectorsTestUtils, DeltaLog, DeltaTestUtilsForTempViews}
import org.apache.spark.sql.delta.DeltaTestUtils.BOOLEAN_DOMAIN
import org.apache.spark.sql.delta.actions.DeletionVectorDescriptor
import org.apache.spark.sql.delta.actions.DeletionVectorDescriptor.EMPTY
import org.apache.spark.sql.delta.deletionvectors.DeletionVectorsSuite._
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.test.DeltaSQLCommandTest
import org.apache.spark.sql.delta.util.JsonUtils
import com.fasterxml.jackson.databind.node.ObjectNode
import org.apache.commons.io.FileUtils

import org.apache.spark.sql.{DataFrame, QueryTest, Row}
import org.apache.spark.sql.catalyst.plans.logical.{AppendData, Subquery}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.test.SharedSparkSession

class DeletionVectorsSuite extends QueryTest
  with SharedSparkSession
  with DeltaSQLCommandTest
  with DeletionVectorsTestUtils
  with DeltaTestUtilsForTempViews {
  import testImplicits._

  test(s"read Delta table with deletion vectors") {
    def verifyVersion(version: Int, expectedData: Seq[Int]): Unit = {
      checkAnswer(
        spark.read.format("delta").option("versionAsOf", version.toString).load(table1Path),
        expectedData.toDF())
    }
    // Verify all versions of the table
    verifyVersion(0, expectedTable1DataV0)
    verifyVersion(1, expectedTable1DataV1)
    verifyVersion(2, expectedTable1DataV2)
    verifyVersion(3, expectedTable1DataV3)
    verifyVersion(4, expectedTable1DataV4)
  }

  test(s"read partitioned Delta table with deletion vectors") {
    def verify(version: Int, expectedData: Seq[Int], filterExp: String = "true"): Unit = {
      val query = spark.read.format("delta")
          .option("versionAsOf", version.toString)
          .load(table3Path)
          .filter(filterExp)
      val expected = expectedData.toDF("id")
          .withColumn("partCol", col("id") % 10)
          .filter(filterExp)

      checkAnswer(query, expected)
    }
    // Verify all versions of the table
    verify(0, expectedTable3DataV0)
    verify(1, expectedTable3DataV1)
    verify(2, expectedTable3DataV2)
    verify(3, expectedTable3DataV3)
    verify(4, expectedTable3DataV4)

    verify(4, expectedTable3DataV4, filterExp = "partCol = 3")
    verify(3, expectedTable3DataV3, filterExp = "partCol = 3 and id > 25")
    verify(1, expectedTable3DataV1, filterExp = "id > 25")
  }

  test("select metadata columns from a Delta table with deletion vectors") {
    assert(spark.read.format("delta").load(table1Path)
      .select("_metadata.file_path").distinct().count() == 22)
  }

  test("throw error when non-pinned TahoeFileIndex snapshot is used") {
    // Corner case where we still have non-pinned TahoeFileIndex when data skipping is disabled
    withSQLConf(DeltaSQLConf.DELTA_STATS_SKIPPING.key -> "false") {
      def assertError(dataFrame: DataFrame): Unit = {
        val ex = intercept[IllegalArgumentException] {
          dataFrame.collect()
        }
        assert(ex.getMessage contains
          "Cannot work with a non-pinned table snapshot of the TahoeFileIndex")
      }
      assertError(spark.read.format("delta").load(table1Path))
      assertError(spark.read.format("delta").option("versionAsOf", "2").load(table1Path))
    }
  }

  test("read Delta table with deletion vectors with a filter") {
    checkAnswer(
      spark.read.format("delta").load(table1Path).where("value in (300, 787, 239)"),
      // 300 is removed in the final table
      Seq(787, 239).toDF())
  }

  test("read Delta table with DV for a select files") {
    val deltaLog = DeltaLog.forTable(spark, table1Path)
    val snapshot = deltaLog.unsafeVolatileSnapshot

    // Select a subset of files with DVs and specific value range, this is just to test
    // that reading these files will respect the DVs
    var rowCount = 0L
    var deletedRowCount = 0L
    val selectFiles = snapshot.allFiles.collect().filter(
      addFile => {
        val stats = JsonUtils.mapper.readTree(addFile.stats).asInstanceOf[ObjectNode]
        // rowCount += stats.get("rowCount")
        val min = stats.get("minValues").get("value").toString
        val max = stats.get("maxValues").get("value").toString
        val selected = (min == "18" && max == "1988") ||
            (min == "33" && max == "1995") || (min == "13" && max == "1897")
        // TODO: these steps will be easier and also change (depending upon tightBounds value) once
        // we expose more methods on AddFile as part of the data skipping changes with DVs
        if (selected) {
          rowCount += stats.get("numRecords").asInt(0)
          deletedRowCount += Option(addFile.deletionVector).getOrElse(EMPTY).cardinality
        }
        selected
      }
      ).toSeq
    assert(selectFiles.filter(_.deletionVector != null).size > 1) // make at least one file has DV

    assert(deltaLog.createDataFrame(snapshot, selectFiles).count() == rowCount - deletedRowCount)
  }

  for (optimizeMetadataQuery <- BOOLEAN_DOMAIN)
    test("read Delta tables with DVs in subqueries: " +
      s"metadataQueryOptimizationEnabled=$optimizeMetadataQuery") {
      withSQLConf(DeltaSQLConf.DELTA_OPTIMIZE_METADATA_QUERY_ENABLED.key ->
        optimizeMetadataQuery.toString) {
        val table1 = s"delta.`${new File(table1Path).getAbsolutePath}`"
        val table2 = s"delta.`${new File(table2Path).getAbsolutePath}`"

        def assertQueryResult(query: String, expected1: Int, expected2: Int): Unit = {
          val df = spark.sql(query)
          assertPlanContains(df, Subquery.getClass.getSimpleName.stripSuffix("$"))
          val actual = df.collect()(0) // fetch only row in the result
          assert(actual === Row(expected1, expected2))
        }

        // same table used twice in the query
        val query1 = s"SELECT (SELECT COUNT(*) FROM $table1), (SELECT COUNT(*) FROM $table1)"
        assertQueryResult(query1, expectedTable1DataV4.size, expectedTable1DataV4.size)

        // two tables used in the query
        val query2 = s"SELECT (SELECT COUNT(*) FROM $table1), (SELECT COUNT(*) FROM $table2)"
        assertQueryResult(query2, expectedTable1DataV4.size, expectedTable2DataV1.size)
      }
    }

  test("insert into Delta table with DVs") {
    withTempDir { tempDir =>
      val source1 = new File(table1Path)
      val source2 = new File(table2Path)
      val target = new File(tempDir, "insertTest")

      // Copy the source2 DV table to a temporary directory
      FileUtils.copyDirectory(source1, target)

      // Insert data from source2 into source1 (copied to target)
      // This blind append generates a plan with `V2WriteCommand` which is a corner
      // case in `PrepareDeltaScan` rule
      val insertDf = spark.sql(s"INSERT INTO TABLE delta.`${target.getAbsolutePath}` " +
        s"SELECT * FROM delta.`${source2.getAbsolutePath}`")
      // [[AppendData]] is one of the [[V2WriteCommand]] subtypes
      assertPlanContains(insertDf, AppendData.getClass.getSimpleName.stripSuffix("$"))

      val dataInTarget = spark.sql(s"SELECT * FROM delta.`${target.getAbsolutePath}`")

      // Make sure the number of rows is correct.
      for (metadataQueryOptimization <- BOOLEAN_DOMAIN) {
        withSQLConf(DeltaSQLConf.DELTA_OPTIMIZE_METADATA_QUERY_ENABLED.key ->
          metadataQueryOptimization.toString) {
          assert(dataInTarget.count() == expectedTable2DataV1.size + expectedTable1DataV4.size)
        }
      }

      // Make sure the contents are the same
      checkAnswer(
        dataInTarget,
        spark.sql(
          s"SELECT * FROM delta.`${source1.getAbsolutePath}` UNION ALL " +
          s"SELECT * FROM delta.`${source2.getAbsolutePath}`")
      )
    }
  }

  test("DELETE with DVs - on a table with no prior DVs") {
    withDeletionVectorsEnabled() {
      withTempDir { dirName =>
        // Create table with 500 files of 2 rows each.
        val numFiles = 500
        val path = dirName.getAbsolutePath
        spark.range(0, 1000, step = 1, numPartitions = numFiles)
          .write.format("delta").save(path)
        val tableName = s"delta.`$path`"

        val beforeDeleteFiles = DeltaLog.forTable(spark, path)
            .unsafeVolatileSnapshot.allFiles.collect().map(_.path)

        val numFilesWithDVs = 100
        val numDeletedRows = numFilesWithDVs * 1
        spark.sql(s"DELETE FROM $tableName WHERE id % 2 = 0 AND id < 200")

        // Verify the expected no. of deletion vectors and deleted rows according to DV cardinality
        val allFiles = DeltaLog.forTable(spark, path).unsafeVolatileSnapshot.allFiles.collect()
        assert(allFiles.size === numFiles)
        assert(allFiles.filter(_.deletionVector != null).size === numFilesWithDVs)
        assert(
          allFiles.filter(_.deletionVector != null).map(_.deletionVector.cardinality).sum ==
          numDeletedRows)

        val afterDeleteFiles = allFiles.map(_.path)
        // make sure the data file list is the same
        assert(beforeDeleteFiles === afterDeleteFiles)

        // Contents after the DELETE are as expected
        checkAnswer(
          spark.sql(s"SELECT * FROM $tableName"),
          Seq.range(0, 1000).filterNot(Seq.range(start = 0, end = 200, step = 2).contains(_)).toDF()
        )
      }
    }
  }

  test("DELETE with DVs - existing table already has DVs") {
    withSQLConf(DeltaSQLConf.DELETE_USE_PERSISTENT_DELETION_VECTORS.key -> "true") {
      withTempDir { tempDir =>
        val source = new File(table1Path)
        val target = new File(tempDir, "deleteTest")

        // Copy the source DV table to a temporary directory
        FileUtils.copyDirectory(source, target)

        val targetPath = s"delta.`${target.getAbsolutePath}`"
        val dataToRemove = Seq(1999, 299, 7, 87, 867, 456)
        val existingDVs = getFilesWithDeletionVectors(DeltaLog.forTable(spark, target))

        spark.sql(s"DELETE FROM $targetPath WHERE value in (${dataToRemove.mkString(",")})")

        // Check new DVs are created
        val newDVs = getFilesWithDeletionVectors(DeltaLog.forTable(spark, target))
        // expect the new DVs contain extra entries for the deleted rows.
        assert(
          existingDVs.map(_.deletionVector.cardinality).sum + dataToRemove.size ===
          newDVs.map(_.deletionVector.cardinality).sum
        )

        // Check the data is valid
        val expectedTable1DataV5 = expectedTable1DataV4.filterNot(e => dataToRemove.contains(e))
        checkAnswer(spark.sql(s"SELECT * FROM $targetPath"), expectedTable1DataV5.toDF())
      }
    }
  }

  private def assertPlanContains(queryDf: DataFrame, expected: String): Unit = {
    val optimizedPlan = queryDf.queryExecution.analyzed.toString()
    assert(optimizedPlan.contains(expected), s"Plan is missing `$expected`: $optimizedPlan")
  }
}

object DeletionVectorsSuite {
  val table1Path = "src/test/resources/delta/table-with-dv-large"
  // Table at version 0: contains [0, 2000)
  val expectedTable1DataV0 = Seq.range(0, 2000)
  // Table at version 1: removes rows with id = 0, 180, 300, 700, 1800
  val v1Removed = Set(0, 180, 300, 700, 1800)
  val expectedTable1DataV1 = expectedTable1DataV0.filterNot(e => v1Removed.contains(e))
  // Table at version 2: inserts rows with id = 300, 700
  val v2Added = Set(300, 700)
  val expectedTable1DataV2 = expectedTable1DataV1 ++ v2Added
  // Table at version 3: removes rows with id = 300, 250, 350, 900, 1353, 1567, 1800
  val v3Removed = Set(300, 250, 350, 900, 1353, 1567, 1800)
  val expectedTable1DataV3 = expectedTable1DataV2.filterNot(e => v3Removed.contains(e))
  // Table at version 4: inserts rows with id = 900, 1567
  val v4Added = Set(900, 1567)
  val expectedTable1DataV4 = expectedTable1DataV3 ++ v4Added

  val table2Path = "src/test/resources/delta/table-with-dv-small"
  // Table at version 0: contains 0 - 9
  val expectedTable2DataV0 = Seq(0, 1, 2, 3, 4, 5, 6, 7, 8, 9)
  // Table at version 1: removes rows 0 and 9
  val expectedTable2DataV1 = Seq(1, 2, 3, 4, 5, 6, 7, 8)

  val table3Path = "src/test/resources/delta/partitioned-table-with-dv-large"
  // Table at version 0: contains [0, 2000)
  val expectedTable3DataV0 = Seq.range(0, 2000)
  // Table at version 1: removes rows with id = (0, 180, 308, 225, 756, 1007, 1503)
  val table3V1Removed = Set(0, 180, 308, 225, 756, 1007, 1503)
  val expectedTable3DataV1 = expectedTable3DataV0.filterNot(e => table3V1Removed.contains(e))
  // Table at version 2: inserts rows with id = 308, 756
  val table3V2Added = Set(308, 756)
  val expectedTable3DataV2 = expectedTable3DataV1 ++ table3V2Added
  // Table at version 3: removes rows with id = (300, 257, 399, 786, 1353, 1567, 1800)
  val table3V3Removed = Set(300, 257, 399, 786, 1353, 1567, 1800)
  val expectedTable3DataV3 = expectedTable3DataV2.filterNot(e => table3V3Removed.contains(e))
  // Table at version 4: inserts rows with id = 1353, 1567
  val table3V4Added = Set(1353, 1567)
  val expectedTable3DataV4 = expectedTable3DataV3 ++ table3V4Added
}
