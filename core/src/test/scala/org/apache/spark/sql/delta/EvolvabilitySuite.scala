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

import org.apache.spark.sql.delta.test.DeltaSQLCommandTest
import org.apache.spark.sql.delta.util.{FileNames, JsonUtils}
import org.apache.hadoop.fs.Path

// scalastyle:off import.ordering.noEmptyLine
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.functions.typedLit
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.util.Utils

class EvolvabilitySuite extends EvolvabilitySuiteBase with DeltaSQLCommandTest {

  import testImplicits._

  test("delta 0.1.0") {
    testEvolvability("src/test/resources/delta/delta-0.1.0")
  }

  test("delta 0.1.0 - case sensitivity enabled") {
    withSQLConf(SQLConf.CASE_SENSITIVE.key -> "true") {
      testEvolvability("src/test/resources/delta/delta-0.1.0")
    }
  }

  test("serialized partition values must contain null values") {
    val tempDir = Utils.createTempDir().toString
    val df1 = spark.range(5).withColumn("part", typedLit[String](null))
    val df2 = spark.range(5).withColumn("part", typedLit("1"))
    df1.union(df2).coalesce(1).write.partitionBy("part").format("delta").save(tempDir)

    // Clear the cache
    DeltaLog.clearCache()
    val deltaLog = DeltaLog.forTable(spark, tempDir)

    val dataThere = deltaLog.snapshot.allFiles.collect().forall { addFile =>
      if (!addFile.partitionValues.contains("part")) {
        fail(s"The partition values: ${addFile.partitionValues} didn't contain the column 'part'.")
      }
      val value = addFile.partitionValues("part")
      value === null || value === "1"
    }

    assert(dataThere, "Partition values didn't match with null or '1'")

    // Check serialized JSON as well
    val contents = deltaLog.store.read(
      FileNames.deltaFile(deltaLog.logPath, 0L),
      deltaLog.newDeltaHadoopConf())
    assert(contents.exists(_.contains(""""part":null""")), "null value should be written in json")
  }

  testQuietly("parse old version CheckpointMetaData") {
    assert(JsonUtils.mapper.readValue[CheckpointMetaData]("""{"version":1,"size":1}""")
      === CheckpointMetaData(1, 1, None, None, None))
  }

  test("parse partial version CheckpointMetaData") {
    assert(JsonUtils.mapper.readValue[CheckpointMetaData](
      """{"version":1,"size":1,"sizeInBytes":100}""")
      === CheckpointMetaData(1, 1, None, Some(100L), None))
  }

  // Following tests verify that operations on Delta table won't fail when there is an
  // unknown column in Delta files and checkpoints.
  // The modified Delta files and checkpoints with an extra column is generated by
  // `EvolvabilitySuiteBase.generateTransactionLogWithExtraColumn()`

  test("transaction log schema evolvability - batch read") {
    testLogSchemaEvolvability(
      (path: String) => { spark.read.format("delta").load(path).collect() }
    )
  }

  test("transaction log schema evolvability - batch write") {
    testLogSchemaEvolvability(
      (path: String) => {
        (10 until 20).map(num => (num, num)).toDF("key", "value")
          .write.format("delta").mode("append").save(path)
        spark.read.format("delta").load(path).collect()
      }
    )
  }

  test("transaction log schema evolvability - streaming read") {
    testLogSchemaEvolvability(
      (path: String) => {
        val query = spark.readStream.format("delta").load(path).writeStream.format("noop").start()
        try {
          query.processAllAvailable()
        } finally {
          query.stop()
        }
      }
    )
  }

  test("transaction log schema evolvability - streaming write") {
    testLogSchemaEvolvability(
      (path: String) => {
        withTempDir { tempDir =>
          val memStream = MemoryStream[(Int, Int)]
          memStream.addData((11, 11), (12, 12))
          val stream = memStream.toDS().toDF("key", "value")
            .coalesce(1).writeStream
            .format("delta")
            .trigger(Trigger.Once)
            .outputMode("append")
            .option("checkpointLocation", tempDir.getCanonicalPath + "/cp")
            .start(path)
          try {
            stream.processAllAvailable()
          } finally {
            stream.stop()
          }
        }
      }
    )
  }

  test("transaction log schema evolvability - describe commands") {
    testLogSchemaEvolvability(
      (path: String) => {
        spark.sql(s"DESCRIBE delta.`$path`")
        spark.sql(s"DESCRIBE HISTORY delta.`$path`")
        spark.sql(s"DESCRIBE DETAIL delta.`$path`")
      }
    )
  }

  test("transaction log schema evolvability - vacuum") {
    testLogSchemaEvolvability(
      (path: String) => {
        sql(s"VACUUM delta.`$path`")
      }
    )
  }

  test("transaction log schema evolvability - alter table") {
    testLogSchemaEvolvability(
      (path: String) => {
        sql(s"ALTER TABLE delta.`$path` ADD COLUMNS (col int)")
      }
    )
  }

  test("transaction log schema evolvability - delete") {
    testLogSchemaEvolvability(
      (path: String) => { sql(s"DELETE FROM delta.`$path` WHERE key = 1") }
    )
  }

  test("transaction log schema evolvability - update") {
    testLogSchemaEvolvability(
      (path: String) => { sql(s"UPDATE delta.`$path` set value = 100 WHERE key = 1") }
    )
  }

  test("transaction log schema evolvability - merge") {
    testLogSchemaEvolvability(
      (path: String) => {
        withTable("source") {
          Seq((1, 5), (11, 12))
            .toDF("key", "value")
            .write
            .mode("overwrite")
            .format("delta")
            .saveAsTable("source")
          sql(
            s"""
               |MERGE INTO delta.`$path` tgrt
               |USING source src
               |ON src.key = tgrt.key
               |WHEN MATCHED THEN
               |  UPDATE SET key = 20 + src.key, value = 20 + src.value
               |WHEN NOT MATCHED THEN
               |  INSERT (key, value) VALUES (src.key + 5, src.value + 10)
           """.stripMargin
          )
        }
      }
    )
  }
}
