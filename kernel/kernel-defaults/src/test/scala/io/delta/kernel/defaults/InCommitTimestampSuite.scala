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
package io.delta.kernel.defaults

import io.delta.kernel.Operation.CREATE_TABLE
import io.delta.kernel._
import io.delta.kernel.engine.Engine
import io.delta.kernel.internal.actions.SingleAction
import io.delta.kernel.internal.fs.Path
import io.delta.kernel.internal.util.FileNames
import io.delta.kernel.internal.util.Utils.singletonCloseableIterator
import io.delta.kernel.types.IntegerType.INTEGER
import io.delta.kernel.types._
import io.delta.kernel.utils.CloseableIterable.emptyIterable

import java.util.Optional
import scala.collection.JavaConverters._
import scala.collection.immutable.Seq

class InCommitTimestampSuite extends DeltaTableWriteSuiteBase {
  val testEngineInfo = "test-engine"
  val testSchema = new StructType().add("id", INTEGER)

  private def getInCommitTimestamp(engine: Engine, table: Table, version: Long): Long = {
    val logPath = new Path(table.getPath(engine), "_delta_log")
    val file = engine
      .getFileSystemClient
      .listFrom(FileNames.listingPrefix(logPath, version)).next
    val row =
      engine.getJsonHandler.readJsonFiles(
        singletonCloseableIterator(file),
        SingleAction.FULL_SCHEMA,
        Optional.empty()).toSeq.map(batch => batch.getRows.next).head
    val commitInfoOrd = row.getSchema.indexOf("commitInfo")
    val commitInfoRow = row.getStruct(commitInfoOrd)
    val inCommitTimestampOrd = commitInfoRow.getSchema.indexOf("inCommitTimestamp")
    commitInfoRow.getLong(inCommitTimestampOrd)
  }

  test("Enable ICT on commit 0") {
    withTempDirAndEngine { (tablePath, engine) =>
      val beforeCommitAttemptStartTime = System.currentTimeMillis
      val table = Table.forPath(engine, tablePath)
      val txnBuilder = table.createTransactionBuilder(engine, testEngineInfo, CREATE_TABLE)

      val txn = txnBuilder
        .withSchema(engine, testSchema)
        .build(engine)

      txn.commit(engine, emptyIterable())

      assert(getInCommitTimestamp(engine, table, 0) >= beforeCommitAttemptStartTime)
    }
  }
}
