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

package io.delta.kernel.defaults.internal.coordinatedcommits

import io.delta.kernel.defaults.DeltaTableWriteSuiteBase
import io.delta.kernel.defaults.internal.logstore.LogStoreProvider
import io.delta.kernel.defaults.utils.TestRow
import io.delta.kernel.internal.TableConfig._
import io.delta.kernel.Table
import io.delta.kernel.engine.Engine
import io.delta.kernel.internal.SnapshotImpl
import io.delta.kernel.internal.actions.{CommitInfo, Metadata, Protocol}
import io.delta.storage.commit.{Commit, CommitCoordinatorClient}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

import java.util
import java.util.Collections
import scala.collection.convert.ImplicitConversions.`iterator asScala`
import scala.collection.JavaConverters._

class CoordinatedCommitsSuite extends DeltaTableWriteSuiteBase
    with CoordinatedCommitsTestUtils {

  val hadoopConf = new Configuration()
  def commit(
    logPath: Path,
    tableConf: util.Map[String, String],
    version: Long,
    timestamp: Long,
    commit: util.List[String],
    metadata: Metadata,
    commitCoordinatorClient: CommitCoordinatorClient): Commit = {
    val logStore = LogStoreProvider.getLogStore(hadoopConf, logPath.toUri.getScheme)
    val updatedCommitInfo = CommitInfo.empty().withTimestamp(timestamp)
    val updatedActions = if (version == 0) {
      getUpdatedActionsForZerothCommit(updatedCommitInfo, metadata)
    } else {
      getUpdatedActionsForNonZerothCommit(updatedCommitInfo)
    }
    commitCoordinatorClient.commit(
      logStore,
      hadoopConf,
      logPath,
      tableConf,
      version,
      commit.iterator(),
      updatedActions).getCommit
  }

  def writeCommitZero(engine: Engine, logPath: Path, commit: util.List[String]): Unit = {
    createLogPath(engine, logPath)
    val logStore = LogStoreProvider.getLogStore(hadoopConf, logPath.toUri.getScheme)
    logStore.write(
      CoordinatedCommitsUtils.getHadoopDeltaFile(logPath, 0),
      commit.iterator(),
      true,
      hadoopConf)
  }

  test("cold snapshot initialization") {
    val builder = new InMemoryCommitCoordinatorBuilder(10)
    val commitCoordinatorClient = builder.build(Collections.emptyMap())
    CommitCoordinatorProvider.registerBuilder(builder)
    withTempDirAndEngine { (tablePath, engine) =>
      engine.getFileSystemClient.mkdirs(tablePath)
      val logPath = new Path("file:" + tablePath, "_delta_log")
      val table = Table.forPath(engine, tablePath)
      spark.range(0, 10).write.format("delta").mode("overwrite").save(tablePath) // version 0
      checkAnswer(
        spark.sql(s"SELECT * FROM delta.`$tablePath`").collect().map(TestRow(_)),
        (0L to 9L).map(TestRow(_)))
      spark.range(10, 20).write.format("delta").mode("overwrite").save(tablePath) // version 1
      spark.range(20, 30).write.format("delta").mode("append").save(tablePath) // version 2
      checkAnswer(
        spark.sql(s"SELECT * FROM delta.`$tablePath`").collect().map(TestRow(_)),
        (10L to 29L).map(TestRow(_)))

      var tableConf: util.Map[String, String] = null
      val logStore = LogStoreProvider.getLogStore(hadoopConf, logPath.toUri.getScheme)
      (0 to 2).foreach{ version =>
        val delta = CoordinatedCommitsUtils.getHadoopDeltaFile(logPath, version)
        var rows = logStore.read(delta, hadoopConf).toList
        val snapshot = table.getSnapshotAsOfVersion(engine, version).asInstanceOf[SnapshotImpl]
        val metadata = snapshot.getMetadata.withNewConfiguration(
          Map(COORDINATED_COMMITS_COORDINATOR_NAME.getKey -> "in-memory",
            COORDINATED_COMMITS_COORDINATOR_CONF.getKey -> "{}").asJava)
        rows = rows.map(row => {
          if (row.contains("metaData")) {
            row.replace(
              "\"configuration\":{}",
              "\"configuration\":{\"coordinatedCommits.commitCoordinatorConf-preview\":\"{}\"," +
                "\"delta.coordinatedCommits.commitCoordinator-preview\":\"in-memory\"}")
          } else {
            row
          }
        })

        if (version == 0) {
          tableConf = commitCoordinatorClient.registerTable(
            logPath,
            -1L,
            CoordinatedCommitsUtils.convertMetadataToAbstractMetadata(metadata),
            CoordinatedCommitsUtils.convertProtocolToAbstractProtocol(new Protocol(3, 7)))
          writeCommitZero(engine, logPath, rows.asJava)
        } else {
          commit(
            logPath, tableConf, version, version, rows.asJava, metadata, commitCoordinatorClient)
        }
      }
      for (version <- 1 to 2) {
        logPath.getFileSystem(hadoopConf).delete(
          CoordinatedCommitsUtils.getHadoopDeltaFile(logPath, version))
      }
      val snapshot = table.getLatestSnapshot(engine)
      val result = readSnapshot(snapshot, snapshot.getSchema(engine), null, null, engine)
      checkAnswer(result, (10L to 29L).map(TestRow(_)))
    }
  }
}
