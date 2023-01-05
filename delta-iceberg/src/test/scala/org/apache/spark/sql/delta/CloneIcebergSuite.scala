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

// scalastyle:off import.ordering.noEmptyLine
import java.sql.Date

import scala.collection.JavaConverters._
import scala.util.Try

import org.apache.spark.sql.delta.schema.SchemaMergingUtils
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.stats.StatisticsCollection
import org.apache.iceberg
import org.apache.iceberg.hadoop.HadoopTables

import org.apache.spark.sql.{AnalysisException, DataFrame, QueryTest, Row}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.util.DateTimeUtils.{stringToDate, toJavaDate}
import org.apache.spark.sql.functions.{col, date_trunc, from_json, lit, struct, substring, trunc}
import org.apache.spark.sql.types.{LongType, StringType, StructType}
import org.apache.spark.unsafe.types.UTF8String
// scalastyle:on import.ordering.noEmptyLine

trait CloneIcebergSuiteBase extends QueryTest
  with ConvertIcebergToDeltaUtils {

  override def beforeAll(): Unit = {
    super.beforeAll()
    spark.conf.set(DeltaSQLConf.DELTA_CONVERT_ICEBERG_PARTITION_EVOLUTION_ENABLED.key, "true")
  }

  protected val cloneTable = "clone"

  private def toDate(date: String): Date = {
    toJavaDate(stringToDate(UTF8String.fromString(date)).get)
  }

  protected def physicalNamesAreEqual(
    sourceSchema: StructType, targetSchema: StructType): Boolean = {

    val sourcePathToPhysicalName = SchemaMergingUtils.explode(sourceSchema).map {
      case (path, field) => path -> DeltaColumnMapping.getPhysicalName(field)
    }.toMap

    val targetPathToPhysicalName = SchemaMergingUtils.explode(targetSchema).map {
      case (path, field) => path -> DeltaColumnMapping.getPhysicalName(field)
    }.toMap

    targetPathToPhysicalName.foreach {
      case (path, physicalName) =>
        if (!sourcePathToPhysicalName.contains(path) ||
            physicalName != sourcePathToPhysicalName(path)) {
          return false
        }
    }

    sourcePathToPhysicalName.size == targetPathToPhysicalName.size
  }

  protected def testIceberg(testName: String)(f: (String, String) => Unit): Unit = {
    test(s"$testName") {
      Seq(table, s"iceberg.`$tablePath`") foreach { source => f("SHALLOW", source) }
    }
  }

  testIceberg("table with deleted files") { (mode, source) =>
    withTable(table, cloneTable) {
      spark.sql(
        s"""CREATE TABLE $table (id bigint, data string)
           |USING iceberg PARTITIONED BY (data)""".stripMargin)
      spark.sql(s"INSERT INTO $table VALUES (1, 'a'), (2, 'b'), (3, 'c')")
      spark.sql(s"DELETE FROM $table WHERE data > 'a'")
      checkAnswer(spark.sql(s"SELECT * from $table"), Row(1, "a") :: Nil)

      spark.sql(s"CREATE TABLE $cloneTable $mode CLONE $source")

      assert(SchemaMergingUtils.equalsIgnoreCaseAndCompatibleNullability(
        DeltaLog.forTable(spark, TableIdentifier(cloneTable)).snapshot.schema,
        new StructType().add("id", LongType).add("data", StringType)))

      checkAnswer(spark.table(cloneTable), Row(1, "a") :: Nil)
    }
  }

  protected def runCreateOrReplace(mode: String, source: String): DataFrame = {
    Try(spark.sql(s"DELETE FROM $cloneTable"))
    spark.sql(s"CREATE OR REPLACE TABLE $cloneTable $mode CLONE $source")
  }

  testIceberg("table with renamed columns") { (mode, source) =>
    withTable(table, cloneTable) {
      spark.sql(
        s"""CREATE TABLE $table (id bigint, data string)
           |USING iceberg PARTITIONED BY (data)""".stripMargin)
      spark.sql(s"INSERT INTO $table VALUES (1, 'a'), (2, 'b')")
      spark.sql("ALTER TABLE local.db.table RENAME COLUMN id TO id2")
      spark.sql(s"INSERT INTO $table VALUES (3, 'c')")

      // Parquet files still have the old schema
        assert(
          SchemaMergingUtils.equalsIgnoreCaseAndCompatibleNullability(
            spark.read.format("parquet").load(tablePath + "/data").schema,
            new StructType().add("id", LongType).add("data", StringType)))

      runCreateOrReplace(mode, source)
      // The converted delta table will get the updated schema
      assert(
        SchemaMergingUtils.equalsIgnoreCaseAndCompatibleNullability(
          DeltaLog.forTable(spark, TableIdentifier(cloneTable)).snapshot.schema,
          new StructType().add("id2", LongType).add("data", StringType)))

      checkAnswer(spark.table(cloneTable), Row(1, "a") :: Row(2, "b") :: Row(3, "c") :: Nil)
    }
  }

  testIceberg("create or replace table - same schema") { (mode, source) =>
    withTable(table, cloneTable) {
      spark.sql(
        s"""CREATE TABLE $table (id bigint, data string)
           |USING iceberg PARTITIONED BY (data)""".stripMargin)

      // Add some rows to check the initial CLONE.
      spark.sql(s"INSERT INTO $table VALUES (1, 'a'), (2, 'b')")
      runCreateOrReplace(mode, source)
      checkAnswer(spark.table(cloneTable), Row(1, "a") :: Row(2, "b") :: Nil)

      // Add more rows to check incremental update with REPLACE.
      spark.sql(s"INSERT INTO $table VALUES (3, 'c')")
      runCreateOrReplace(mode, source)
      checkAnswer(spark.table(cloneTable), Row(1, "a") :: Row(2, "b") :: Row(3, "c") :: Nil)
    }
  }

  testIceberg("create or replace table - renamed column") { (mode, source) =>
    withTable(table, cloneTable) {
      spark.sql(
        s"""CREATE TABLE $table (id bigint, data string)
           |USING iceberg PARTITIONED BY (data)""".stripMargin)

      spark.sql(s"INSERT INTO $table VALUES (1, 'a'), (2, 'b')")
      runCreateOrReplace(mode, source)
      assert(
        SchemaMergingUtils.equalsIgnoreCaseAndCompatibleNullability(
          DeltaLog.forTable(spark, TableIdentifier(cloneTable)).snapshot.schema,
          new StructType().add("id", LongType).add("data", StringType)))

      checkAnswer(spark.table(cloneTable), Row(1, "a") :: Row(2, "b") :: Nil)

      // Rename column 'id' into column 'id2'.
      spark.sql("ALTER TABLE local.db.table RENAME COLUMN id TO id2")
      spark.sql(s"INSERT INTO $table VALUES (3, 'c')")

      // Update the cloned delta table with REPLACE.
      runCreateOrReplace(mode, source)
      assert(
        SchemaMergingUtils.equalsIgnoreCaseAndCompatibleNullability(
          DeltaLog.forTable(spark, TableIdentifier(cloneTable)).snapshot.schema,
          new StructType().add("id2", LongType).add("data", StringType)))

      checkAnswer(spark.table(cloneTable), Row(1, "a") :: Row(2, "b") :: Row(3, "c") :: Nil)
    }
  }

  testIceberg("create or replace table - deleted rows") { (mode, source) =>
    withTable(table, cloneTable) {
      spark.sql(
        s"""CREATE TABLE $table (id bigint, data string)
           |USING iceberg PARTITIONED BY (data)""".stripMargin)

      spark.sql(s"INSERT INTO $table VALUES (1, 'a'), (2, 'b'), (3, 'c')")
      runCreateOrReplace(mode, source)
      checkAnswer(spark.table(cloneTable), Row(1, "a") :: Row(2, "b") :: Row(3, "c") :: Nil)

      // Delete some rows from the iceberg table.
      spark.sql(s"DELETE FROM $table WHERE data > 'a'")
      checkAnswer(
        spark.sql(s"SELECT * from $table"), Row(1, "a") :: Nil)

      runCreateOrReplace(mode, source)
      checkAnswer(spark.table(cloneTable), Row(1, "a") :: Nil)
    }
  }

  testIceberg("create or replace table - schema with nested column") { (mode, source) =>
    withTable(table, cloneTable) {
      spark.sql(
        s"""CREATE TABLE $table (id bigint, person struct<name:string,phone:int>)
           |USING iceberg PARTITIONED BY (truncate(person.name, 2))""".stripMargin)

      spark.sql(s"INSERT INTO $table VALUES (1, ('AaAaAa', 10)), (2, ('BbBbBb', 20))")
      runCreateOrReplace(mode, source)
      checkAnswer(
        spark.table(cloneTable),
        Row(1, Row("AaAaAa", 10), "Aa") :: Row(2, Row("BbBbBb", 20), "Bb") :: Nil)

      val deltaLog = DeltaLog.forTable(spark, TableIdentifier(cloneTable))
      val schemaBefore = deltaLog.update().schema

      spark.sql(s"INSERT INTO $table VALUES (3, ('AaZzZz', 30)), (4, ('CcCcCc', 40))")
      runCreateOrReplace(mode, source)
      checkAnswer(
        spark.table(cloneTable),
        Row(1, Row("AaAaAa", 10), "Aa") :: Row(2, Row("BbBbBb", 20), "Bb") ::
          Row(3, Row("AaZzZz", 30), "Aa") :: Row(4, Row("CcCcCc", 40), "Cc") :: Nil)

      assert(physicalNamesAreEqual(schemaBefore, deltaLog.update().schema))
    }
  }

  test("negative case: select from iceberg table using path") {
    withTable(table) {
        val ae = intercept[AnalysisException] {
          sql(s"SELECT * FROM iceberg.`$tablePath`")
        }
      assert(ae.getMessage.contains("table does not support batch scan"))
    }
  }

  testIceberg("create or replace table - add partition field") { (mode, source) =>
    withTable(table, cloneTable) {
      spark.sql(
        s"""CREATE TABLE $table (date date, id bigint, category string, price double)
           | USING iceberg PARTITIONED BY (date)""".stripMargin)

      // scalastyle:off deltahadoopconfiguration
      val hadoopTables = new HadoopTables(spark.sessionState.newHadoopConf())
      // scalastyle:on deltahadoopconfiguration
      val icebergTable = hadoopTables.load(tablePath)
      val icebergTableSchema = iceberg.spark.SparkSchemaUtil.convert(icebergTable.schema())

      val df1 = spark.createDataFrame(
        Seq(
          Row(toDate("2022-01-01"), 1L, "toy", 2.5D),
          Row(toDate("2022-01-01"), 2L, "food", 0.6D),
          Row(toDate("2022-02-05"), 3L, "food", 1.4D),
          Row(toDate("2022-02-05"), 4L, "toy", 10.2D)).asJava,
        icebergTableSchema)

      df1.writeTo(table).append()

      runCreateOrReplace(mode, source)
      val deltaLog = DeltaLog.forTable(spark, TableIdentifier(cloneTable))
      assert(deltaLog.snapshot.metadata.partitionColumns == Seq("date"))
      checkAnswer(spark.table(cloneTable), df1)

      // Add a new partition field from the existing column "category"
      icebergTable.refresh()
      icebergTable.updateSpec().addField("category").commit()

      // Invalidate cache and load the updated partition spec
      spark.sql(s"REFRESH TABLE $table")
      val df2 = spark.createDataFrame(
        Seq(
          Row(toDate("2022-02-05"), 5L, "toy", 5.8D),
          Row(toDate("2022-06-04"), 6L, "toy", 20.1D)).asJava,
        icebergTableSchema)

      df2.writeTo(table).append()

      runCreateOrReplace(mode, source)
      assert(deltaLog.update().metadata.partitionColumns == Seq("date", "category"))
      // Old data of cloned Delta table has null on the new partition field.
      checkAnswer(spark.table(cloneTable), df1.withColumn("category", lit(null)).union(df2))
      // Iceberg table projects existing value of old data to the new partition field though.
      checkAnswer(spark.sql(s"SELECT * FROM $table"), df1.union(df2))
    }
  }

  testIceberg("create or replace table - remove partition field") { (mode, source) =>
    withTable(table, cloneTable) {
      spark.sql(
        s"""CREATE TABLE $table (date date, id bigint, category string, price double)
           | USING iceberg PARTITIONED BY (date)""".stripMargin)

      // scalastyle:off deltahadoopconfiguration
      val hadoopTables = new HadoopTables(spark.sessionState.newHadoopConf())
      // scalastyle:on deltahadoopconfiguration
      val icebergTable = hadoopTables.load(tablePath)
      val icebergTableSchema = iceberg.spark.SparkSchemaUtil.convert(icebergTable.schema())

      val df1 = spark.createDataFrame(
        Seq(
          Row(toDate("2022-01-01"), 1L, "toy", 2.5D),
          Row(toDate("2022-01-01"), 2L, "food", 0.6D),
          Row(toDate("2022-02-05"), 3L, "food", 1.4D),
          Row(toDate("2022-02-05"), 4L, "toy", 10.2D)).asJava,
        icebergTableSchema)

      df1.writeTo(table).append()

      runCreateOrReplace(mode, source)
      val deltaLog = DeltaLog.forTable(spark, TableIdentifier(cloneTable))
      assert(deltaLog.snapshot.metadata.partitionColumns == Seq("date"))
      checkAnswer(spark.table(cloneTable), df1)

      // Remove the partition field "date"
      icebergTable.refresh()
      icebergTable.updateSpec().removeField("date").commit()

      // Invalidate cache and load the updated partition spec
      spark.sql(s"REFRESH TABLE $table")
      val df2 = spark.createDataFrame(
        Seq(
          Row(toDate("2022-02-05"), 5L, "toy", 5.8D),
          Row(toDate("2022-06-04"), 6L, "toy", 20.1D)).asJava,
        icebergTableSchema)

      df2.writeTo(table).append()

      runCreateOrReplace(mode, source)
      assert(deltaLog.update().metadata.partitionColumns.isEmpty)
      // Both cloned Delta table and Iceberg table has data for the removed partition field.
      checkAnswer(spark.table(cloneTable), df1.union(df2))
      checkAnswer(spark.table(cloneTable), spark.sql(s"SELECT * FROM $table"))
    }
  }

  testIceberg("create or replace table - replace partition field") { (mode, source) =>
    withTable(table, cloneTable) {
      spark.sql(
        s"""CREATE TABLE $table (date date, id bigint, category string, price double)
           | USING iceberg PARTITIONED BY (date)""".stripMargin)

      // scalastyle:off deltahadoopconfiguration
      val hadoopTables = new HadoopTables(spark.sessionState.newHadoopConf())
      // scalastyle:on deltahadoopconfiguration
      val icebergTable = hadoopTables.load(tablePath)
      val icebergTableSchema = iceberg.spark.SparkSchemaUtil.convert(icebergTable.schema())

      val df1 = spark.createDataFrame(
        Seq(
          Row(toDate("2022-01-01"), 1L, "toy", 2.5D),
          Row(toDate("2022-01-01"), 2L, "food", 0.6D),
          Row(toDate("2022-02-05"), 3L, "food", 1.4D),
          Row(toDate("2022-02-05"), 4L, "toy", 10.2D)).asJava,
        icebergTableSchema)

      df1.writeTo(table).append()

      runCreateOrReplace(mode, source)
      val deltaLog = DeltaLog.forTable(spark, TableIdentifier(cloneTable))
      assert(deltaLog.snapshot.metadata.partitionColumns == Seq("date"))
      checkAnswer(spark.table(cloneTable), df1)

      // Replace the partition field "date" with a transformed field "month(date)"
      icebergTable.refresh()
      icebergTable.updateSpec().removeField("date")
        .addField(iceberg.expressions.Expressions.month("date")).commit()

      // Invalidate cache and load the updated partition spec
      spark.sql(s"REFRESH TABLE $table")
      val df2 = spark.createDataFrame(
        Seq(
          Row(toDate("2022-02-05"), 5L, "toy", 5.8D),
          Row(toDate("2022-06-04"), 6L, "toy", 20.1D)).asJava,
        icebergTableSchema)

      df2.writeTo(table).append()

      runCreateOrReplace(mode, source)
      assert(deltaLog.update().metadata.partitionColumns == Seq("date_month"))
      // Old data of cloned Delta table has null on the new partition field.
      checkAnswer(spark.table(cloneTable),
        df1.withColumn("date_month", lit(null))
          .union(df2.withColumn("date_month", substring(col("date") cast "String", 1, 7))))
      // The new partition field is a hidden metadata column in Iceberg.
      checkAnswer(
        spark.table(cloneTable).drop("date_month"),
        spark.sql(s"SELECT * FROM $table"))
    }
  }
}

class CloneIcebergSuite extends CloneIcebergSuiteBase

