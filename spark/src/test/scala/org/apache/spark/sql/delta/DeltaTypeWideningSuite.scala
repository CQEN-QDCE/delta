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

import java.util.concurrent.TimeUnit

import org.apache.spark.sql.delta.actions.AddFile
import org.apache.spark.sql.delta.catalog.DeltaTableV2
import org.apache.spark.sql.delta.commands.AlterTableDropFeatureDeltaCommand
import org.apache.spark.sql.delta.rowtracking.RowTrackingTestUtils
import org.apache.spark.sql.delta.test.DeltaSQLCommandTest
import org.apache.spark.sql.delta.util.JsonUtils
import org.apache.hadoop.fs.Path
import org.scalatest.BeforeAndAfterEach

import org.apache.spark.{SparkConf, SparkException}
import org.apache.spark.sql.{AnalysisException, DataFrame, Encoder, QueryTest, Row}
import org.apache.spark.sql.errors.QueryErrorsBase
import org.apache.spark.sql.execution.datasources.parquet.ParquetTest
import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types._
import org.apache.spark.util.ManualClock

/**
 * Suite covering the type widening table feature.
 */
class DeltaTypeWideningSuite
  extends QueryTest
    with ParquetTest
    with DeltaDMLTestUtils
    with RowTrackingTestUtils
    with DeltaSQLCommandTest
    with DeltaTypeWideningTestMixin
    with DeltaTypeWideningAlterTableTests
    with DeltaTypeWideningNestedFieldsTests
    with DeltaTypeWideningMetadataTests
    with DeltaTypeWideningTableFeatureTests

/**
 * Test mixin that enables type widening by default for all tests in the suite.
 */
trait DeltaTypeWideningTestMixin extends SharedSparkSession {
  protected override def sparkConf: SparkConf = {
    super.sparkConf
      .set(DeltaConfigs.ENABLE_TYPE_WIDENING.defaultTablePropertyKey, "true")
      // Ensure we don't silently cast test inputs to null on overflow.
      .set(SQLConf.ANSI_ENABLED.key, "true")
  }

  /** Enable (or disable) type widening for the table under the given path. */
  protected def enableTypeWidening(tablePath: String, enabled: Boolean = true): Unit =
    sql(s"ALTER TABLE delta.`$tablePath` " +
          s"SET TBLPROPERTIES('${DeltaConfigs.ENABLE_TYPE_WIDENING.key}' = '${enabled.toString}')")

  /** Short-hand to create type widening metadata for struct fields. */
  protected def typeWideningMetadata(
      version: Long,
      from: AtomicType,
      to: AtomicType,
      path: Seq[String] = Seq.empty): Metadata =
    new MetadataBuilder()
      .putMetadataArray(
        "delta.typeChanges", Array(TypeChange(version, from, to, path).toMetadata))
      .build()
}

/**
 * Trait collecting supported and unsupported type change test cases.
 */
trait DeltaTypeWideningTestCases { self: SharedSparkSession =>
  import testImplicits._

  /**
   * Represents the input of a type change test.
   * @param fromType         The original type of the column 'value' in the test table.
   * @param toType           The type to use when changing the type of column 'value'.
   */
  abstract class TypeEvolutionTestCase(
      val fromType: DataType,
      val toType: DataType) {
    /** The initial values to insert with type `fromType` in column 'value' after table creation. */
    def initialValuesDF: DataFrame
    /** Additional values to insert after changing the type of the column 'value' to `toType`. */
    def additionalValuesDF: DataFrame
    /** Expected content of the table after inserting the additional values. */
    def expectedResult: DataFrame
  }

  /**
   * Represents the input of a supported type change test. Handles converting the test values from
   * scala types to a dataframe.
   */
  case class SupportedTypeEvolutionTestCase[
      FromType  <: DataType, ToType <: DataType,
      FromVal: Encoder, ToVal: Encoder
    ](
      override val fromType: FromType,
      override val toType: ToType,
      initialValues: Seq[FromVal],
      additionalValues: Seq[ToVal]
  ) extends TypeEvolutionTestCase(fromType, toType) {
    override def initialValuesDF: DataFrame =
      initialValues.toDF("value").select($"value".cast(fromType))

    override def additionalValuesDF: DataFrame =
      additionalValues.toDF("value").select($"value".cast(toType))

    override def expectedResult: DataFrame =
      initialValuesDF.union(additionalValuesDF).select($"value".cast(toType))
  }

  // Type changes that are supported by all Parquet readers. Byte, Short, Int are all stored as
  // INT32 in parquet so these changes are guaranteed to be supported.
  protected val supportedTestCases: Seq[TypeEvolutionTestCase] = Seq(
    SupportedTypeEvolutionTestCase(ByteType, ShortType,
      Seq(1, -1, Byte.MinValue, Byte.MaxValue, null.asInstanceOf[Byte]),
      Seq(4, -4, Short.MinValue, Short.MaxValue, null.asInstanceOf[Short])),
    SupportedTypeEvolutionTestCase(ByteType, IntegerType,
      Seq(1, -1, Byte.MinValue, Byte.MaxValue, null.asInstanceOf[Byte]),
      Seq(4, -4, Int.MinValue, Int.MaxValue, null.asInstanceOf[Int])),
    SupportedTypeEvolutionTestCase(ShortType, IntegerType,
      Seq(1, -1, Short.MinValue, Short.MaxValue, null.asInstanceOf[Short]),
      Seq(4, -4, Int.MinValue, Int.MaxValue, null.asInstanceOf[Int]))
  )

  /**
   * Represents the input of an unsupported type change test. Handles converting the test values
   * from scala types to a dataframe. Additional values to insert are always empty since the type
   * change is expected to fail.
   */
  case class UnsupportedTypeEvolutionTestCase[
    FromType  <: DataType, ToType <: DataType, FromVal : Encoder](
      override val fromType: FromType,
      override val toType: ToType,
      initialValues: Seq[FromVal]) extends TypeEvolutionTestCase(fromType, toType) {
    override def initialValuesDF: DataFrame =
      initialValues.toDF("value").select($"value".cast(fromType))

    override def additionalValuesDF: DataFrame =
      spark.createDataFrame(
        sparkContext.emptyRDD[Row],
        new StructType().add(StructField("value", toType)))

    override def expectedResult: DataFrame =
      initialValuesDF.select($"value".cast(toType))
  }

  // Test type changes that aren't supported.
  protected val unsupportedTestCases: Seq[TypeEvolutionTestCase] = Seq(
    UnsupportedTypeEvolutionTestCase(IntegerType, ByteType,
      Seq(1, 2, Int.MinValue)),
    UnsupportedTypeEvolutionTestCase(LongType, IntegerType,
      Seq(4, 5, Long.MaxValue)),
    UnsupportedTypeEvolutionTestCase(DoubleType, FloatType,
      Seq(987654321.987654321d, Double.NaN, Double.NegativeInfinity,
        Double.PositiveInfinity, Double.MinPositiveValue,
        Double.MinValue, Double.MaxValue)),
    UnsupportedTypeEvolutionTestCase(ByteType, DecimalType(2, 0),
      Seq(1, -1, Byte.MinValue)),
    UnsupportedTypeEvolutionTestCase(ShortType, DecimalType(4, 0),
      Seq(1, -1, Short.MinValue)),
    UnsupportedTypeEvolutionTestCase(IntegerType, DecimalType(9, 0),
      Seq(1, -1, Int.MinValue)),
    UnsupportedTypeEvolutionTestCase(LongType, DecimalType(19, 0),
      Seq(1, -1, Long.MinValue)),
    UnsupportedTypeEvolutionTestCase(TimestampNTZType, DateType,
      Seq("2020-03-17 15:23:15", "2023-12-31 23:59:59", "0001-01-01 00:00:00")),
    // Reduce scale
    UnsupportedTypeEvolutionTestCase(DecimalType(Decimal.MAX_INT_DIGITS, 2),
      DecimalType(Decimal.MAX_INT_DIGITS, 3),
      Seq(BigDecimal("-67.89"), BigDecimal("9" * (Decimal.MAX_INT_DIGITS - 2) + ".99"))),
    // Reduce precision
    UnsupportedTypeEvolutionTestCase(DecimalType(Decimal.MAX_INT_DIGITS, 2),
      DecimalType(Decimal.MAX_INT_DIGITS - 1, 2),
      Seq(BigDecimal("-67.89"), BigDecimal("9" * (Decimal.MAX_INT_DIGITS - 2) + ".99"))),
    // Reduce precision & scale
    UnsupportedTypeEvolutionTestCase(DecimalType(Decimal.MAX_LONG_DIGITS, 2),
      DecimalType(Decimal.MAX_INT_DIGITS - 1, 1),
      Seq(BigDecimal("-67.89"), BigDecimal("9" * (Decimal.MAX_LONG_DIGITS - 2) + ".99"))),
    // Increase scale more than precision
    UnsupportedTypeEvolutionTestCase(DecimalType(Decimal.MAX_INT_DIGITS, 2),
      DecimalType(Decimal.MAX_INT_DIGITS + 1, 4),
      Seq(BigDecimal("-67.89"), BigDecimal("9" * (Decimal.MAX_INT_DIGITS - 2) + ".99"))),
    // Smaller scale and larger precision.
    UnsupportedTypeEvolutionTestCase(DecimalType(Decimal.MAX_LONG_DIGITS, 2),
      DecimalType(Decimal.MAX_INT_DIGITS + 3, 1),
      Seq(BigDecimal("-67.89"), BigDecimal("9" * (Decimal.MAX_LONG_DIGITS - 2) + ".99")))
  )
}

/**
 * Trait collecting a subset of tests providing core coverage for type widening using ALTER TABLE
 * CHANGE COLUMN TYPE.
 */
trait DeltaTypeWideningAlterTableTests extends QueryErrorsBase with DeltaTypeWideningTestCases {
  self: QueryTest with ParquetTest with DeltaTypeWideningTestMixin with DeltaDMLTestUtils =>

  import testImplicits._

  for {
    testCase <- supportedTestCases
    partitioned <- BOOLEAN_DOMAIN
  } {
    test(s"type widening ${testCase.fromType.sql} -> ${testCase.toType.sql}, " +
      s"partitioned=$partitioned") {
      def writeData(df: DataFrame): Unit = if (partitioned) {
        // The table needs to have at least 1 non-partition column, use a dummy one.
        append(df.withColumn("dummy", lit(1)), partitionBy = Seq("value"))
      } else {
        append(df)
      }

      writeData(testCase.initialValuesDF)
      sql(s"ALTER TABLE delta.`$tempPath` CHANGE COLUMN value TYPE ${testCase.toType.sql}")
      withAllParquetReaders {
        assert(readDeltaTable(tempPath).schema("value").dataType === testCase.toType)
        checkAnswer(readDeltaTable(tempPath).select("value").sort("value"),
          testCase.initialValuesDF.select($"value".cast(testCase.toType)).sort("value"))
      }
      writeData(testCase.additionalValuesDF)
      withAllParquetReaders {
        checkAnswer(
          readDeltaTable(tempPath).select("value").sort("value"),
          testCase.expectedResult.sort("value"))
      }
    }
  }

  for {
    testCase <- unsupportedTestCases
    partitioned <- BOOLEAN_DOMAIN
  } {
    test(s"unsupported type changes ${testCase.fromType.sql} -> ${testCase.toType.sql}, " +
      s"partitioned=$partitioned") {
      if (partitioned) {
        // The table needs to have at least 1 non-partition column, use a dummy one.
        append(testCase.initialValuesDF.withColumn("dummy", lit(1)), partitionBy = Seq("value"))
      } else {
        append(testCase.initialValuesDF)
      }
      sql(s"ALTER TABLE delta.`$tempPath` " +
        s"SET TBLPROPERTIES('delta.feature.timestampNtz' = 'supported')")

      val alterTableSql =
        s"ALTER TABLE delta.`$tempPath` CHANGE COLUMN value TYPE ${testCase.toType.sql}"
      checkError(
        exception = intercept[AnalysisException] {
          sql(alterTableSql)
        },
        errorClass = "NOT_SUPPORTED_CHANGE_COLUMN",
        sqlState = None,
        parameters = Map(
          "table" -> s"`spark_catalog`.`delta`.`$tempPath`",
          "originName" -> toSQLId("value"),
          "originType" -> toSQLType(testCase.fromType),
          "newName" -> toSQLId("value"),
          "newType" -> toSQLType(testCase.toType)),
        context = ExpectedContext(
          fragment = alterTableSql,
          start = 0,
          stop = alterTableSql.length - 1)
      )
    }
  }

  test("type widening using ALTER TABLE REPLACE COLUMNS") {
    append(Seq(1, 2).toDF("value").select($"value".cast(ShortType)))
    assert(readDeltaTable(tempPath).schema === new StructType().add("value", ShortType))
    sql(s"ALTER TABLE delta.`$tempPath` REPLACE COLUMNS (value INT)")
    assert(readDeltaTable(tempPath).schema ===
      new StructType()
        .add("value", IntegerType, nullable = true, metadata = new MetadataBuilder()
          .putMetadataArray("delta.typeChanges", Array(
            new MetadataBuilder()
              .putString("toType", "integer")
              .putString("fromType", "short")
              .putLong("tableVersion", 1)
              .build()
          )).build()))
    checkAnswer(readDeltaTable(tempPath), Seq(Row(1), Row(2)))
    append(Seq(3, 4).toDF("value"))
    checkAnswer(readDeltaTable(tempPath), Seq(Row(1), Row(2), Row(3), Row(4)))
  }

}

/**
 * Tests covering type changes on nested fields in structs, maps and arrays.
 */
trait DeltaTypeWideningNestedFieldsTests {
  self: QueryTest with ParquetTest with DeltaDMLTestUtils with DeltaTypeWideningTestMixin
    with SharedSparkSession =>

  import testImplicits._

  /** Create a table with a struct, map and array for each test. */
  protected def createNestedTable(): Unit = {
    sql(s"CREATE TABLE delta.`$tempPath` " +
      "(s struct<a: byte>, m map<byte, short>, a array<short>) USING DELTA")
    append(Seq((1, 2, 3, 4))
      .toDF("a", "b", "c", "d")
      .selectExpr(
        "named_struct('a', cast(a as byte)) as s",
        "map(cast(b as byte), cast(c as short)) as m",
        "array(cast(d as short)) as a"))

    assert(readDeltaTable(tempPath).schema === new StructType()
      .add("s", new StructType().add("a", ByteType))
      .add("m", MapType(ByteType, ShortType))
      .add("a", ArrayType(ShortType)))
  }

  test("unsupported ALTER TABLE CHANGE COLUMN on non-leaf fields") {
    createNestedTable()
    // Running ALTER TABLE CHANGE COLUMN on non-leaf fields is invalid.
    var alterTableSql = s"ALTER TABLE delta.`$tempPath` CHANGE COLUMN s TYPE struct<a: short>"
    checkError(
      exception = intercept[AnalysisException] { sql(alterTableSql) },
      errorClass = "CANNOT_UPDATE_FIELD.STRUCT_TYPE",
      parameters = Map(
        "table" -> s"`spark_catalog`.`delta`.`$tempPath`",
        "fieldName" -> "`s`"
      ),
      context = ExpectedContext(
        fragment = alterTableSql,
        start = 0,
        stop = alterTableSql.length - 1)
    )

    alterTableSql = s"ALTER TABLE delta.`$tempPath` CHANGE COLUMN m TYPE map<int, int>"
    checkError(
      exception = intercept[AnalysisException] { sql(alterTableSql) },
      errorClass = "CANNOT_UPDATE_FIELD.MAP_TYPE",
      parameters = Map(
        "table" -> s"`spark_catalog`.`delta`.`$tempPath`",
        "fieldName" -> "`m`"
      ),
      context = ExpectedContext(
        fragment = alterTableSql,
        start = 0,
        stop = alterTableSql.length - 1)
    )

    alterTableSql = s"ALTER TABLE delta.`$tempPath` CHANGE COLUMN a TYPE array<int>"
    checkError(
      exception = intercept[AnalysisException] { sql(alterTableSql) },
      errorClass = "CANNOT_UPDATE_FIELD.ARRAY_TYPE",
      parameters = Map(
        "table" -> s"`spark_catalog`.`delta`.`$tempPath`",
        "fieldName" -> "`a`"
      ),
      context = ExpectedContext(
        fragment = alterTableSql,
        start = 0,
        stop = alterTableSql.length - 1)
    )
  }

  test("type widening with ALTER TABLE on nested fields") {
    createNestedTable()
    sql(s"ALTER TABLE delta.`$tempPath` CHANGE COLUMN s.a TYPE short")
    sql(s"ALTER TABLE delta.`$tempPath` CHANGE COLUMN m.key TYPE int")
    sql(s"ALTER TABLE delta.`$tempPath` CHANGE COLUMN m.value TYPE int")
    sql(s"ALTER TABLE delta.`$tempPath` CHANGE COLUMN a.element TYPE int")

    assert(readDeltaTable(tempPath).schema === new StructType()
      .add("s", new StructType()
        .add("a", ShortType, nullable = true, metadata = new MetadataBuilder()
          .putMetadataArray("delta.typeChanges", Array(
            new MetadataBuilder()
              .putString("toType", "short")
              .putString("fromType", "byte")
              .putLong("tableVersion", 2)
              .build()
          )).build()))
      .add("m", MapType(IntegerType, IntegerType), nullable = true, metadata = new MetadataBuilder()
          .putMetadataArray("delta.typeChanges", Array(
            new MetadataBuilder()
              .putString("toType", "integer")
              .putString("fromType", "byte")
              .putLong("tableVersion", 3)
              .putString("fieldPath", "key")
              .build(),
            new MetadataBuilder()
              .putString("toType", "integer")
              .putString("fromType", "short")
              .putLong("tableVersion", 4)
              .putString("fieldPath", "value")
              .build()
          )).build())
      .add("a", ArrayType(IntegerType), nullable = true, metadata = new MetadataBuilder()
          .putMetadataArray("delta.typeChanges", Array(
            new MetadataBuilder()
              .putString("toType", "integer")
              .putString("fromType", "short")
              .putLong("tableVersion", 5)
              .putString("fieldPath", "element")
              .build()
          )).build()))

    append(Seq((5, 6, 7, 8))
      .toDF("a", "b", "c", "d")
        .selectExpr("named_struct('a', cast(a as short)) as s", "map(b, c) as m", "array(d) as a"))

    checkAnswer(
      readDeltaTable(tempPath),
      Seq((1, 2, 3, 4), (5, 6, 7, 8))
        .toDF("a", "b", "c", "d")
        .selectExpr("named_struct('a', cast(a as short)) as s", "map(b, c) as m", "array(d) as a"))
  }

  test("type widening using ALTER TABLE REPLACE COLUMNS on nested fields") {
    createNestedTable()
    sql(s"ALTER TABLE delta.`$tempPath` REPLACE COLUMNS " +
      "(s struct<a: short>, m map<int, int>, a array<int>)")
    assert(readDeltaTable(tempPath).schema === new StructType()
      .add("s", new StructType()
        .add("a", ShortType, nullable = true, metadata = new MetadataBuilder()
          .putMetadataArray("delta.typeChanges", Array(
            new MetadataBuilder()
              .putString("toType", "short")
              .putString("fromType", "byte")
              .putLong("tableVersion", 2)
              .build()
          )).build()))
      .add("m", MapType(IntegerType, IntegerType), nullable = true, metadata = new MetadataBuilder()
          .putMetadataArray("delta.typeChanges", Array(
            new MetadataBuilder()
              .putString("toType", "integer")
              .putString("fromType", "byte")
              .putLong("tableVersion", 2)
              .putString("fieldPath", "key")
              .build(),
            new MetadataBuilder()
              .putString("toType", "integer")
              .putString("fromType", "short")
              .putLong("tableVersion", 2)
              .putString("fieldPath", "value")
              .build()
          )).build())
      .add("a", ArrayType(IntegerType), nullable = true, metadata = new MetadataBuilder()
          .putMetadataArray("delta.typeChanges", Array(
            new MetadataBuilder()
              .putString("toType", "integer")
              .putString("fromType", "short")
              .putLong("tableVersion", 2)
              .putString("fieldPath", "element")
              .build()
          )).build()))

    append(Seq((5, 6, 7, 8))
      .toDF("a", "b", "c", "d")
        .selectExpr("named_struct('a', cast(a as short)) as s", "map(b, c) as m", "array(d) as a"))

    checkAnswer(
      readDeltaTable(tempPath),
      Seq((1, 2, 3, 4), (5, 6, 7, 8))
        .toDF("a", "b", "c", "d")
        .selectExpr("named_struct('a', cast(a as short)) as s", "map(b, c) as m", "array(d) as a"))
  }
}

/**
 * Tests related to recording type change information as metadata in the table schema. For
 * lower-level tests, see [[DeltaTypeWideningMetadataSuite]].
 */
trait DeltaTypeWideningMetadataTests {
  self: QueryTest with ParquetTest with DeltaTypeWideningTestMixin with DeltaDMLTestUtils =>

  def testTypeWideningMetadata(name: String)(
      initialSchema: String,
      typeChanges: Seq[(String, String)],
      expectedJsonSchema: String): Unit =
    test(name) {
      sql(s"CREATE TABLE delta.`$tempPath` ($initialSchema) USING DELTA")
      typeChanges.foreach { case (fieldName, newType) =>
        sql(s"ALTER TABLE delta.`$tempPath` CHANGE COLUMN $fieldName TYPE $newType")
      }

      // Parse the schemas as JSON to ignore whitespaces and field order.
      val actualSchema = JsonUtils.fromJson[Map[String, Any]](readDeltaTable(tempPath).schema.json)
      val expectedSchema = JsonUtils.fromJson[Map[String, Any]](expectedJsonSchema)
      assert(actualSchema === expectedSchema,
        s"${readDeltaTable(tempPath).schema.prettyJson} did not equal $expectedJsonSchema"
      )
    }

  testTypeWideningMetadata("change top-level column type short->int")(
    initialSchema = "a short",
    typeChanges = Seq("a" -> "int"),
    expectedJsonSchema =
      """{
      "type": "struct",
      "fields": [{
        "name": "a",
        "type": "integer",
        "nullable": true,
        "metadata": {
          "delta.typeChanges": [{
            "toType": "integer",
            "fromType": "short",
            "tableVersion": 1
          }]
        }
      }]}""".stripMargin)

  testTypeWideningMetadata("change top-level column type twice byte->short->int")(
    initialSchema = "a byte",
    typeChanges = Seq("a" -> "short", "a" -> "int"),
    expectedJsonSchema =
      """{
      "type": "struct",
      "fields": [{
        "name": "a",
        "type": "integer",
        "nullable": true,
        "metadata": {
          "delta.typeChanges": [{
            "toType": "short",
            "fromType": "byte",
            "tableVersion": 1
          },{
            "toType": "integer",
            "fromType": "short",
            "tableVersion": 2
          }]
        }
      }]}""".stripMargin)

  testTypeWideningMetadata("change type in map key and in struct in map value")(
    initialSchema = "a map<byte, struct<b: byte>>",
    typeChanges = Seq("a.key" -> "int", "a.value.b" -> "short"),
    expectedJsonSchema =
      """{
      "type": "struct",
      "fields": [{
        "name": "a",
        "type": {
          "type": "map",
          "keyType": "integer",
          "valueType": {
            "type": "struct",
            "fields": [{
              "name": "b",
              "type": "short",
              "nullable": true,
              "metadata": {
                "delta.typeChanges": [{
                  "toType": "short",
                  "fromType": "byte",
                  "tableVersion": 2
                }]
              }
            }]
          },
          "valueContainsNull": true
        },
        "nullable": true,
        "metadata": {
          "delta.typeChanges": [{
            "toType": "integer",
            "fromType": "byte",
            "tableVersion": 1,
            "fieldPath": "key"
          }]
        }
      }
    ]}""".stripMargin)


  testTypeWideningMetadata("change type in array and in struct in array")(
    initialSchema = "a array<byte>, b array<struct<c: short>>",
    typeChanges = Seq("a.element" -> "short", "b.element.c" -> "int"),
    expectedJsonSchema =
      """{
      "type": "struct",
      "fields": [{
        "name": "a",
        "type": {
          "type": "array",
          "elementType": "short",
          "containsNull": true
        },
        "nullable": true,
        "metadata": {
          "delta.typeChanges": [{
            "toType": "short",
            "fromType": "byte",
            "tableVersion": 1,
            "fieldPath": "element"
          }]
        }
      },
      {
        "name": "b",
        "type": {
          "type": "array",
          "elementType":{
            "type": "struct",
            "fields": [{
              "name": "c",
              "type": "integer",
              "nullable": true,
              "metadata": {
                "delta.typeChanges": [{
                  "toType": "integer",
                  "fromType": "short",
                  "tableVersion": 2
                }]
              }
            }]
          },
          "containsNull": true
        },
        "nullable": true,
        "metadata": { }
      }
    ]}""".stripMargin)
}

/**
 * Tests covering adding and removing the type widening table feature. Dropping the table feature
 * also includes rewriting data files with the old type and removing type widening metadata.
 */
trait DeltaTypeWideningTableFeatureTests extends BeforeAndAfterEach {
  self: QueryTest
    with ParquetTest
    with DeltaDMLTestUtils
    with RowTrackingTestUtils
    with DeltaTypeWideningTestMixin =>

  import testImplicits._

  /** Clock used to advance past the retention period when dropping the table feature. */
  var clock: ManualClock = _

  override protected def beforeEach(): Unit = {
    super.beforeEach()
    clock = new ManualClock(System.currentTimeMillis())
    // Override the (cached) delta log with one using our manual clock.
    DeltaLog.clearCache()
    deltaLog = DeltaLog.forTable(spark, new Path(tempPath), clock)
  }

  def isTypeWideningSupported: Boolean = {
    TypeWidening.isSupported(deltaLog.update().protocol)
  }

  def isTypeWideningEnabled: Boolean = {
    val snapshot = deltaLog.update()
    TypeWidening.isEnabled(snapshot.protocol, snapshot.metadata)
  }

  /** Expected outcome of dropping the type widening table feature. */
  object ExpectedOutcome extends Enumeration {
    val SUCCESS, FAIL_CURRENT_VERSION_USES_FEATURE, FAIL_HISTORICAL_VERSION_USES_FEATURE = Value
  }

  /** Helper method to drop the type widening table feature and check for an expected outcome. */
  def dropTableFeature(expectedOutcome: ExpectedOutcome.Value): Unit = {
    // Need to directly call ALTER TABLE command to pass our deltaLog with manual clock.
    val dropFeature = AlterTableDropFeatureDeltaCommand(
      DeltaTableV2(spark, deltaLog.dataPath),
      TypeWideningTableFeature.name)

    expectedOutcome match {
      case ExpectedOutcome.SUCCESS =>
        dropFeature.run(spark)
      case ExpectedOutcome.FAIL_CURRENT_VERSION_USES_FEATURE =>
        checkError(
          exception = intercept[DeltaTableFeatureException] { dropFeature.run(spark) },
          errorClass = "DELTA_FEATURE_DROP_WAIT_FOR_RETENTION_PERIOD",
          parameters = Map(
            "feature" -> TypeWideningTableFeature.name,
            "logRetentionPeriodKey" -> DeltaConfigs.LOG_RETENTION.key,
            "logRetentionPeriod" -> DeltaConfigs.LOG_RETENTION
              .fromMetaData(deltaLog.unsafeVolatileMetadata).toString,
            "truncateHistoryLogRetentionPeriod" ->
              DeltaConfigs.TABLE_FEATURE_DROP_TRUNCATE_HISTORY_LOG_RETENTION
                .fromMetaData(deltaLog.unsafeVolatileMetadata).toString)
        )
      case ExpectedOutcome.FAIL_HISTORICAL_VERSION_USES_FEATURE =>
        checkError(
          exception = intercept[DeltaTableFeatureException] { dropFeature.run(spark) },
          errorClass = "DELTA_FEATURE_DROP_HISTORICAL_VERSIONS_EXIST",
          parameters = Map(
            "feature" -> TypeWideningTableFeature.name,
            "logRetentionPeriodKey" -> DeltaConfigs.LOG_RETENTION.key,
            "logRetentionPeriod" -> DeltaConfigs.LOG_RETENTION
              .fromMetaData(deltaLog.unsafeVolatileMetadata).toString,
            "truncateHistoryLogRetentionPeriod" ->
              DeltaConfigs.TABLE_FEATURE_DROP_TRUNCATE_HISTORY_LOG_RETENTION
                .fromMetaData(deltaLog.unsafeVolatileMetadata).toString)
        )
    }
  }

  /**
   * Use this after dropping the table feature to artificially move the current time to after
   * the table retention period.
   */
  def advancePastRetentionPeriod(): Unit = {
    clock.advance(
      deltaLog.deltaRetentionMillis(deltaLog.update().metadata) +
        TimeUnit.MINUTES.toMillis(5))
  }

  def addSingleFile[T: Encoder](values: Seq[T], dataType: DataType): Unit =
      append(values.toDF("a").select(col("a").cast(dataType)).repartition(1))

  /** Get the number of AddFile actions committed since the given table version (included). */
  def getNumAddFilesSinceVersion(version: Long): Long =
    deltaLog
      .getChanges(startVersion = version)
      .flatMap { case (_, actions) => actions }
      .collect { case a: AddFile => a }
      .size

  test("enable type widening at table creation then disable it") {
    sql(s"CREATE TABLE delta.`$tempPath` (a int) USING DELTA " +
      s"TBLPROPERTIES ('${DeltaConfigs.ENABLE_TYPE_WIDENING.key}' = 'true')")
    assert(isTypeWideningSupported)
    assert(isTypeWideningEnabled)
    enableTypeWidening(tempPath, enabled = false)
    assert(isTypeWideningSupported)
    assert(!isTypeWideningEnabled)
  }

  test("enable type widening after table creation then disable it") {
    sql(s"CREATE TABLE delta.`$tempPath` (a int) USING DELTA " +
      s"TBLPROPERTIES ('${DeltaConfigs.ENABLE_TYPE_WIDENING.key}' = 'false')")
    assert(!isTypeWideningSupported)
    assert(!isTypeWideningEnabled)
    // Setting the property to false shouldn't add the table feature if it's not present.
    enableTypeWidening(tempPath, enabled = false)
    assert(!isTypeWideningSupported)
    assert(!isTypeWideningEnabled)

    enableTypeWidening(tempPath)
    assert(isTypeWideningSupported)
    assert(isTypeWideningEnabled)
    enableTypeWidening(tempPath, enabled = false)
    assert(isTypeWideningSupported)
    assert(!isTypeWideningEnabled)
  }

  test("set table property to incorrect value") {
    val ex = intercept[IllegalArgumentException] {
      sql(s"CREATE TABLE delta.`$tempPath` (a int) USING DELTA " +
        s"TBLPROPERTIES ('${DeltaConfigs.ENABLE_TYPE_WIDENING.key}' = 'bla')")
    }
    assert(ex.getMessage.contains("For input string: \"bla\""))
    sql(s"CREATE TABLE delta.`$tempPath` (a int) USING DELTA " +
       s"TBLPROPERTIES ('${DeltaConfigs.ENABLE_TYPE_WIDENING.key}' = 'false')")
    checkError(
      exception = intercept[SparkException] {
        sql(s"ALTER TABLE delta.`$tempPath` " +
          s"SET TBLPROPERTIES ('${DeltaConfigs.ENABLE_TYPE_WIDENING.key}' = 'bla')")
      },
      errorClass = "_LEGACY_ERROR_TEMP_2045",
      parameters = Map(
        "message" -> "For input string: \"bla\""
      )
    )
    assert(!isTypeWideningSupported)
    assert(!isTypeWideningEnabled)
  }

  test("change column type without table feature") {
    sql(s"CREATE TABLE delta.`$tempPath` (a TINYINT) USING DELTA " +
      s"TBLPROPERTIES ('${DeltaConfigs.ENABLE_TYPE_WIDENING.key}' = 'false')")

    checkError(
      exception = intercept[AnalysisException] {
        sql(s"ALTER TABLE delta.`$tempPath` CHANGE COLUMN a TYPE SMALLINT")
      },
      errorClass = "DELTA_UNSUPPORTED_ALTER_TABLE_CHANGE_COL_OP",
      parameters = Map(
        "fieldPath" -> "a",
        "oldField" -> "TINYINT",
        "newField" -> "SMALLINT"
      )
    )
  }

  test("change column type with type widening table feature supported but table property set to " +
    "false") {
    sql(s"CREATE TABLE delta.`$tempPath` (a SMALLINT) USING DELTA")
    sql(s"ALTER TABLE delta.`$tempPath` " +
      s"SET TBLPROPERTIES ('${DeltaConfigs.ENABLE_TYPE_WIDENING.key}' = 'false')")

    checkError(
      exception = intercept[AnalysisException] {
        sql(s"ALTER TABLE delta.`$tempPath` CHANGE COLUMN a TYPE INT")
      },
      errorClass = "DELTA_UNSUPPORTED_ALTER_TABLE_CHANGE_COL_OP",
      parameters = Map(
        "fieldPath" -> "a",
        "oldField" -> "SMALLINT",
        "newField" -> "INT"
      )
    )
  }

  test("no-op type changes are always allowed") {
    sql(s"CREATE TABLE delta.`$tempPath` (a int) USING DELTA " +
      s"TBLPROPERTIES ('${DeltaConfigs.ENABLE_TYPE_WIDENING.key}' = 'false')")
    sql(s"ALTER TABLE delta.`$tempPath` CHANGE COLUMN a TYPE INT")
    enableTypeWidening(tempPath, enabled = true)
    sql(s"ALTER TABLE delta.`$tempPath` CHANGE COLUMN a TYPE INT")
    enableTypeWidening(tempPath, enabled = false)
    sql(s"ALTER TABLE delta.`$tempPath` CHANGE COLUMN a TYPE INT")
  }

  test("drop unused table feature on empty table") {
    sql(s"CREATE TABLE delta.`$tempPath` (a byte) USING DELTA")
    dropTableFeature(ExpectedOutcome.SUCCESS)
    assert(getNumAddFilesSinceVersion(version = 0) === 0)
    checkAnswer(readDeltaTable(tempPath), Seq.empty)
  }

  // Rewriting the data when dropping the table feature relies on the default row commit version
  // being set even when row tracking isn't enabled.
  for(rowTrackingEnabled <- BOOLEAN_DOMAIN) {
    test(s"drop unused table feature on table with data, rowTrackingEnabled=$rowTrackingEnabled") {
      sql(s"CREATE TABLE delta.`$tempPath` (a byte) USING DELTA")
      addSingleFile(Seq(1, 2, 3), ByteType)
      assert(getNumAddFilesSinceVersion(version = 0) === 1)

      val version = deltaLog.update().version
      dropTableFeature(ExpectedOutcome.SUCCESS)
      assert(getNumAddFilesSinceVersion(version + 1) === 0)
      checkAnswer(readDeltaTable(tempPath), Seq(Row(1), Row(2), Row(3)))
    }

    test(s"drop unused table feature on table with data inserted before adding the table feature," +
      s"rowTrackingEnabled=$rowTrackingEnabled") {
      sql(s"CREATE TABLE delta.`$tempPath` (a byte) USING DELTA " +
        s"TBLPROPERTIES ('${DeltaConfigs.ENABLE_TYPE_WIDENING.key}' = 'false')")
      addSingleFile(Seq(1, 2, 3), ByteType)
      enableTypeWidening(tempPath)
      sql(s"ALTER TABLE delta.`$tempPath` CHANGE COLUMN a TYPE int")
      assert(getNumAddFilesSinceVersion(version = 0) === 1)

      var version = deltaLog.update().version
      dropTableFeature(ExpectedOutcome.FAIL_CURRENT_VERSION_USES_FEATURE)
      assert(getNumAddFilesSinceVersion(version + 1) === 1)
      assert(!TypeWideningMetadata.containsTypeWideningMetadata(deltaLog.update().schema))

      version = deltaLog.update().version
      dropTableFeature(ExpectedOutcome.FAIL_HISTORICAL_VERSION_USES_FEATURE)
      assert(getNumAddFilesSinceVersion(version + 1) === 0)

      version = deltaLog.update().version
      advancePastRetentionPeriod()
      dropTableFeature(ExpectedOutcome.SUCCESS)
      assert(getNumAddFilesSinceVersion(version + 1) === 0)
      checkAnswer(readDeltaTable(tempPath), Seq(Row(1), Row(2), Row(3)))
    }

    test(s"drop table feature on table with data added only after type change, " +
      s"rowTrackingEnabled=$rowTrackingEnabled") {
      sql(s"CREATE TABLE delta.`$tempPath` (a byte) USING DELTA")
      sql(s"ALTER TABLE delta.`$tempPath` CHANGE COLUMN a TYPE int")
      addSingleFile(Seq(1, 2, 3), IntegerType)
      assert(getNumAddFilesSinceVersion(version = 0) === 1)

      // We could actually drop the table feature directly here instead of failing by checking that
      // there were no files added before the type change. This may be an expensive check for a rare
      // scenario so we don't do it.
      var version = deltaLog.update().version
      dropTableFeature(ExpectedOutcome.FAIL_CURRENT_VERSION_USES_FEATURE)
      assert(getNumAddFilesSinceVersion(version + 1) === 0)
      assert(!TypeWideningMetadata.containsTypeWideningMetadata(deltaLog.update().schema))

      version = deltaLog.update().version
      dropTableFeature(ExpectedOutcome.FAIL_HISTORICAL_VERSION_USES_FEATURE)
      assert(getNumAddFilesSinceVersion(version + 1) === 0)

      advancePastRetentionPeriod()
      version = deltaLog.update().version
      dropTableFeature(ExpectedOutcome.SUCCESS)
      assert(getNumAddFilesSinceVersion(version + 1) === 0)
      checkAnswer(readDeltaTable(tempPath), Seq(Row(1), Row(2), Row(3)))
    }

    test(s"drop table feature on table with data added before type change, " +
      s"rowTrackingEnabled=$rowTrackingEnabled") {
      sql(s"CREATE TABLE delta.`$tempDir` (a byte) USING DELTA")
      addSingleFile(Seq(1, 2, 3), ByteType)
      sql(s"ALTER TABLE delta.`$tempDir` CHANGE COLUMN a TYPE int")
      assert(getNumAddFilesSinceVersion(version = 0) === 1)

      var version = deltaLog.update().version
      dropTableFeature(ExpectedOutcome.FAIL_CURRENT_VERSION_USES_FEATURE)
      assert(getNumAddFilesSinceVersion(version + 1) === 1)
      assert(!TypeWideningMetadata.containsTypeWideningMetadata(deltaLog.update().schema))

      version = deltaLog.update().version
      dropTableFeature(ExpectedOutcome.FAIL_HISTORICAL_VERSION_USES_FEATURE)
      assert(getNumAddFilesSinceVersion(version + 1) === 0)

      version = deltaLog.update().version
      advancePastRetentionPeriod()
      dropTableFeature(ExpectedOutcome.SUCCESS)
      assert(getNumAddFilesSinceVersion(version + 1) === 0)
      checkAnswer(readDeltaTable(tempPath), Seq(Row(1), Row(2), Row(3)))
    }

    test(s"drop table feature on table with data added before type change and fully rewritten " +
      s"after, rowTrackingEnabled=$rowTrackingEnabled") {
      sql(s"CREATE TABLE delta.`$tempDir` (a byte) USING DELTA")
      addSingleFile(Seq(1, 2, 3), ByteType)
      sql(s"ALTER TABLE delta.`$tempDir` CHANGE COLUMN a TYPE int")
      sql(s"UPDATE delta.`$tempDir` SET a = a + 10")
      assert(getNumAddFilesSinceVersion(version = 0) === 2)

      var version = deltaLog.update().version
      dropTableFeature(ExpectedOutcome.FAIL_CURRENT_VERSION_USES_FEATURE)
      // The file was already rewritten in UPDATE.
      assert(getNumAddFilesSinceVersion(version + 1) === 0)
      assert(!TypeWideningMetadata.containsTypeWideningMetadata(deltaLog.update().schema))

      version = deltaLog.update().version
      dropTableFeature(ExpectedOutcome.FAIL_HISTORICAL_VERSION_USES_FEATURE)
      assert(getNumAddFilesSinceVersion(version + 1) === 0)

      version = deltaLog.update().version
      advancePastRetentionPeriod()
      dropTableFeature(ExpectedOutcome.SUCCESS)
      assert(getNumAddFilesSinceVersion(version + 1) === 0)
      checkAnswer(readDeltaTable(tempPath), Seq(Row(11), Row(12), Row(13)))
    }

    test(s"drop table feature on table with data added before type change and partially " +
      s"rewritten after, rowTrackingEnabled=$rowTrackingEnabled") {
      withRowTrackingEnabled(rowTrackingEnabled) {
        sql(s"CREATE TABLE delta.`$tempDir` (a byte) USING DELTA")
        addSingleFile(Seq(1, 2, 3), ByteType)
        addSingleFile(Seq(4, 5, 6), ByteType)
        sql(s"ALTER TABLE delta.`$tempDir` CHANGE COLUMN a TYPE int")
        assert(getNumAddFilesSinceVersion(version = 0) === 2)
        sql(s"UPDATE delta.`$tempDir` SET a = a + 10 WHERE a < 4")
        assert(getNumAddFilesSinceVersion(version = 0) === 3)

        var version = deltaLog.update().version
        dropTableFeature(ExpectedOutcome.FAIL_CURRENT_VERSION_USES_FEATURE)
        // One file was already rewritten in UPDATE, leaving 1 file to rewrite.
        assert(getNumAddFilesSinceVersion(version + 1) === 1)
        assert(!TypeWideningMetadata.containsTypeWideningMetadata(deltaLog.update().schema))

        version = deltaLog.update().version
        dropTableFeature(ExpectedOutcome.FAIL_HISTORICAL_VERSION_USES_FEATURE)
        assert(getNumAddFilesSinceVersion(version + 1) === 0)

        version = deltaLog.update().version
        advancePastRetentionPeriod()
        dropTableFeature(ExpectedOutcome.SUCCESS)
        assert(getNumAddFilesSinceVersion(version + 1) === 0)
        checkAnswer(
          readDeltaTable(tempPath),
          Seq(Row(11), Row(12), Row(13), Row(4), Row(5), Row(6)))
      }
    }
  }
}
