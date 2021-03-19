/*
 * Copyright (2020) The Delta Lake Project Authors.
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

import java.util.{Calendar, TimeZone}

import org.apache.spark.sql.delta.DeltaHistoryManager.BufferingLogDeletionIterator
import org.apache.spark.sql.delta.metering.DeltaLogging
import org.apache.commons.lang3.time.DateUtils
import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.spark.sql.delta.util.FileNames.{checkpointVersion, deltaVersion, isCheckpointFile}

/** Cleans up expired Delta table metadata. */
trait MetadataCleanup extends DeltaLogging {
  self: DeltaLog =>

  /** Whether to clean up expired log files and checkpoints. */
  def enableExpiredLogCleanup: Boolean =
    DeltaConfigs.ENABLE_EXPIRED_LOG_CLEANUP.fromMetaData(metadata)

  /**
   * Returns the duration in millis for how long to keep around obsolete logs. We may keep logs
   * beyond this duration until the next calendar day to avoid constantly creating checkpoints.
   */
  def deltaRetentionMillis: Long = {
    val interval = DeltaConfigs.LOG_RETENTION.fromMetaData(metadata)
    DeltaConfigs.getMilliSeconds(interval)
  }

  override def doLogCleanup(): Unit = {
    if (enableExpiredLogCleanup) {
      cleanUpExpiredLogs()
    }
  }

  /** Clean up expired delta and checkpoint logs. Exposed for testing. */
  private[delta] def cleanUpExpiredLogs(): Unit = {
    recordDeltaOperation(this, "delta.log.cleanup") {
      val fileCutOffTime = truncateDay(clock.getTimeMillis() - deltaRetentionMillis).getTime
      val formattedDate = fileCutOffTime.toGMTString
      logInfo(s"Starting the deletion of log files older than $formattedDate")

      var numDeleted = 0
      listExpiredDeltaLogs(fileCutOffTime.getTime).map(_.getPath).foreach { path =>
        // recursive = false
        if (fs.delete(path, false)) numDeleted += 1
      }
      logInfo(s"Deleted $numDeleted log files older than $formattedDate")
    }
  }

  private def getVersion(filePath: Path): Long = {
    if (isCheckpointFile(filePath)) {
      checkpointVersion(filePath)
    } else {
      deltaVersion(filePath)
    }
  }

  private def getMaxVersionToDelete(maxExpiredVersion: Long): Long = {
    maxExpiredVersion - (maxExpiredVersion % self.checkpointInterval)
  }

  private def shouldDeleteExpiredFiles
      (expiredFile : FileStatus, maxVersionToDelete: Long): Boolean = {
    getVersion(expiredFile.getPath) < maxVersionToDelete
  }

  /**
   * Returns an iterator of expired delta logs that can be cleaned up. For a delta log to be
   * considered as expired, it must:
   *  - have a checkpoint file after it
   *  - be older than `fileCutOffTime`
   */
  private def listExpiredDeltaLogs(fileCutOffTime: Long): Seq[FileStatus] = {
    import org.apache.spark.sql.delta.util.FileNames._

    val latestCheckpoint = lastCheckpoint
    if (latestCheckpoint.isEmpty) return Seq.empty
    val threshold = latestCheckpoint.get.version - 1L
    val files = store.listFrom(checkpointPrefix(logPath, 0))
      .filter(f => isCheckpointFile(f.getPath) || isDeltaFile(f.getPath))

    val expiredFiles =
      new BufferingLogDeletionIterator(files, fileCutOffTime, getVersion).toSeq
    val lastExpiredVersion = expiredFiles.lastOption
      .map(file => getVersion(file.getPath)).getOrElse(-1L)

    // scalastyle:off
       println(lastExpiredVersion)
    // scalastyle:on
    val maxVersionToDelete = getMaxVersionToDelete(lastExpiredVersion)
    if(maxVersionToDelete <= 0) {
      Seq()
    }
    else {
      expiredFiles.filter(expiredFile => shouldDeleteExpiredFiles(expiredFile, maxVersionToDelete))
    }
  }

  /** Truncates a timestamp down to the previous midnight and returns the time and a log string */
  private[delta] def truncateDay(timeMillis: Long): Calendar = {
    val date = Calendar.getInstance(TimeZone.getTimeZone("UTC"))
    date.setTimeInMillis(timeMillis)
    DateUtils.truncate(
      date,
      Calendar.DAY_OF_MONTH)
  }
}
