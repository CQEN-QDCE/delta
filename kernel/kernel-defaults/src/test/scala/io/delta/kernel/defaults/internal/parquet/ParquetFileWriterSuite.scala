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
package io.delta.kernel.defaults.internal.parquet;

import io.delta.golden.GoldenTableUtils.goldenTableFile
import io.delta.kernel.data.{ColumnarBatch, FilteredColumnarBatch}
import io.delta.kernel.defaults.utils.{TestRow, TestUtils}
import io.delta.kernel.expressions.Column
import io.delta.kernel.internal.util.Utils.toCloseableIterator
import io.delta.kernel.types._
import io.delta.kernel.utils.{DataFileStatus, FileStatus}
import org.apache.hadoop.fs.Path
import org.scalatest.funsuite.AnyFunSuite

import java.nio.file.{Files, Paths}
import java.util.Optional
import scala.collection.JavaConverters._

/**
 * Test strategy for [[ParquetFileWriter]]
 * <p>
 * Golden tables already have Parquet files containing various supported
 * data types and variations (null, non-nulls, decimal types, nested nested types etc.).
 * We will use these files to simplify the tests for ParquetFileWriter. Alternative is to
 * generate the test data in the tests and try to write as Parquet files, but that would be a lot
 * of test code to cover all the combinations.
 * <p>
 * Using the golden Parquet files in combination with Kernel Parquet reader and Spark Parquet
 * reader we will reduce the test code and also test the inter-working of the Parquet writer with
 * the Parquet readers.
 * <p>
 * High level steps in the test:
 * 1) read data using the Kernel Parquet reader to generate the data in [[ColumnarBatch]]es
 * 2) Optional: filter the data from (1) and generate [[FilteredColumnarBatch]]es
 * 3) write the data back to new Parquet file(s) using the ParquetFileWriter that we are
 * testing. We will test the following variations:
 * 3.1) change target file size and stats collection columns etc.
 * 4) verification
 * 4.1) read the new Parquet file(s) using the Kernel Parquet reader and compare with (2)
 * 4.2) read the new Parquet file(s) using the Spark Parquet reader and compare with (2)
 * 4.3) verify the stats returned in (3) are correct using the Spark Parquet reader
 */
class ParquetFileWriterSuite extends AnyFunSuite with TestUtils {
  val ALL_TYPES_DATA = goldenTableFile("parquet-all-types").toString

  private val ALL_TYPES_FILE_SCHEMA = new StructType()
    .add("byteType", ByteType.BYTE)
    .add("shortType", ShortType.SHORT)
    .add("integerType", IntegerType.INTEGER)
    .add("longType", LongType.LONG)
    .add("floatType", FloatType.FLOAT)
    .add("doubleType", DoubleType.DOUBLE)
    .add("decimal", new DecimalType(10, 2))
    .add("booleanType", BooleanType.BOOLEAN)
    .add("stringType", StringType.STRING)
    .add("binaryType", BinaryType.BINARY)
    .add("dateType", DateType.DATE)
    .add("timestampType", TimestampType.TIMESTAMP)
    .add("nested_struct",
      new StructType()
        .add("aa", StringType.STRING)
        .add("ac",
          new StructType()
            .add("aca", IntegerType.INTEGER)))
    .add("array_of_prims", new ArrayType(IntegerType.INTEGER, true))
    .add("array_of_arrays", new ArrayType(new ArrayType(IntegerType.INTEGER, true), true))
    .add("array_of_structs",
      new ArrayType(
        new StructType()
          .add("ab", LongType.LONG), true))
    .add("map_of_prims", new MapType(IntegerType.INTEGER, LongType.LONG, true))
    .add("map_of_rows",
      new MapType(
        IntegerType.INTEGER,
        new StructType().add("ab", LongType.LONG),
        true))
    .add("map_of_arrays",
      new MapType(
        LongType.LONG,
        new ArrayType(IntegerType.INTEGER, true),
        true))

  Seq(200, 1000, 1048576).foreach { targetFileSize =>
    test(s"write all types - no stats collected - targetFileSize: $targetFileSize") {
      withTempDir { tempPath =>
        val targetDir = tempPath.getAbsolutePath

        val columnarBatches = readParquetFilesUsingKernelAsColumnarBatches(
          ALL_TYPES_DATA, ALL_TYPES_FILE_SCHEMA)

        writeParquetFilesUsingKernel(
          columnarBatches, targetDir, targetFileSize, Seq.empty)

        val expectedNumParquetFiles = targetFileSize match {
          case 200 => 100
          case 1000 => 29
          case 1048576 => 1
          case _ => throw new IllegalArgumentException(s"Invalid targetFileSize: $targetFileSize")
        }
        assert(parquetFileCount(targetDir) === expectedNumParquetFiles)

        verify(targetDir, columnarBatches)
      }
    }
  }

  def verify(actualFileDir: String, expected: Seq[ColumnarBatch]): Unit = {
    val filteredBatches = expected.map(_.toFiltered)
    verifyUsingKernelReader(actualFileDir, filteredBatches)
    verifyUsingSparkReader(actualFileDir, filteredBatches)
  }

  /**
   * Verify the data in the Parquet files located in `actualFileDir` matches the expected data.
   * Use Kernel Parquet reader to read the data from the Parquet files.
   */
  def verifyUsingKernelReader(
    actualFileDir: String,
    expected: Seq[FilteredColumnarBatch]): Unit = {

    val dataSchema = expected.head.getData.getSchema

    val expectedTestRows = expected
      .map(fb => fb.getRows)
      .flatMap(_.toSeq)
      .map(TestRow(_))

    val actualTestRows = readParquetFilesUsingKernel(actualFileDir, dataSchema)

    checkAnswer(actualTestRows, expectedTestRows)
  }

  /**
   * Verify the data in the Parquet files located in `actualFileDir`` matches the expected data.
   * Use Spark Parquet reader to read the data from the Parquet files.
   */
  def verifyUsingSparkReader(
    actualFileDir: String,
    expected: Seq[FilteredColumnarBatch]): Unit = {

    val dataSchema = expected.head.getData.getSchema;

    val expectedTestRows = expected
      .map(fb => fb.getRows)
      .flatMap(_.toSeq)
      .map(TestRow(_))

    val actualTestRows = readParquetFilesUsingSpark(actualFileDir, dataSchema)

    checkAnswer(actualTestRows, expectedTestRows)
  }

  /**
   * Write the [[ColumnarBatch]]es to Parquet files using the ParquetFileWriter and verify the
   * data using the Kernel Parquet reader and Spark Parquet reader.
   */
  def writeParquetFilesUsingKernel(
    data: Seq[ColumnarBatch],
    location: String,
    targetFileSize: Long,
    statsColumns: Seq[Column]): Seq[DataFileStatus] = {
    val parquetWriter = new ParquetFileWriter(
      configuration,
      new Path(location),
      targetFileSize,
      statsColumns.asJava)

    val filteredData = data
      .map(new FilteredColumnarBatch(_, Optional.empty()))

    parquetWriter
      .write(toCloseableIterator(filteredData.asJava.iterator()))
      .toSeq
  }

  def readParquetFilesUsingKernel(
    actualFileDir: String, readSchema: StructType): Seq[TestRow] = {
    val columnarBatches = readParquetFilesUsingKernelAsColumnarBatches(actualFileDir, readSchema)
    columnarBatches.map(_.getRows).flatMap(_.toSeq).map(TestRow(_))
  }

  def readParquetFilesUsingKernelAsColumnarBatches(
    actualFileDir: String, readSchema: StructType): Seq[ColumnarBatch] = {
    val parquetFiles = Files.list(Paths.get(actualFileDir))
      .iterator().asScala
      .map(_.toString)
      .filter(path => path.endsWith(".parquet"))
      .map(path => FileStatus.of(path, 0L, 0L))

    val data = defaultTableClient.getParquetHandler.readParquetFiles(
      toCloseableIterator(parquetFiles.asJava),
      readSchema,
      Optional.empty())

    data.asScala.toSeq
  }

  def readParquetFilesUsingSpark(
    actualFileDir: String, readSchema: StructType): Seq[TestRow] = {
    // Read the parquet files in actionFileDir using Spark Parquet reader
    spark.read
      .format("parquet")
      .parquet(actualFileDir)
      .to(readSchema.toSpark)
      .collect()
      .map(TestRow(_))
  }

  def parquetFileCount(path: String): Long = {
    Files.list(Paths.get(path))
      .iterator().asScala
      .map(_.toString)
      .filter(path => path.endsWith(".parquet"))
      .toSeq.size
  }
}
