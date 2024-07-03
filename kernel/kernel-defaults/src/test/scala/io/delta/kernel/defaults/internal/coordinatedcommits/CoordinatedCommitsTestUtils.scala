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

import java.util
import io.delta.kernel.defaults.internal.logstore.LogStoreProvider
import io.delta.kernel.engine.Engine
import io.delta.kernel.internal.actions.{CommitInfo, Metadata, Protocol}
import io.delta.kernel.internal.TableConfig
import io.delta.storage.commit.{Commit, CommitCoordinatorClient, UpdatedActions}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

import scala.collection.JavaConverters._

trait CoordinatedCommitsTestUtils {

  def createLogPath(engine: Engine, logPath: Path): Unit = {
    // New table, create a delta log directory
    if (!engine.getFileSystemClient.mkdirs(logPath.toString)) {
      throw new RuntimeException("Failed to create delta log directory: " + logPath)
    }
  }

  def getUpdatedActionsForZerothCommit(
    commitInfo: CommitInfo,
    oldMetadata: Metadata = Metadata.empty()): UpdatedActions = {
    val newMetadataConfiguration =
      Map(TableConfig.COORDINATED_COMMITS_COORDINATOR_NAME.getKey -> "in-memory",
        TableConfig.COORDINATED_COMMITS_COORDINATOR_CONF.getKey -> "{}")
    val newMetadata = oldMetadata.withNewConfiguration(newMetadataConfiguration.asJava)
    new UpdatedActions(
      CoordinatedCommitsUtils.convertCommitInfoToAbstractCommitInfo(commitInfo),
      CoordinatedCommitsUtils.convertMetadataToAbstractMetadata(newMetadata),
      CoordinatedCommitsUtils.convertProtocolToAbstractProtocol(Protocol.empty()),
      CoordinatedCommitsUtils.convertMetadataToAbstractMetadata(oldMetadata),
      CoordinatedCommitsUtils.convertProtocolToAbstractProtocol(Protocol.empty()))
  }

  def getUpdatedActionsForNonZerothCommit(commitInfo: CommitInfo): UpdatedActions = {
    val updatedActions = getUpdatedActionsForZerothCommit(commitInfo)
    new UpdatedActions(
      updatedActions.getCommitInfo,
      updatedActions.getNewMetadata,
      updatedActions.getNewProtocol,
      updatedActions.getNewMetadata, // oldMetadata is replaced with newMetadata
      updatedActions.getOldProtocol)
  }
}
