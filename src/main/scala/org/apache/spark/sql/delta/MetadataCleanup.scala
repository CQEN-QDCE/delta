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
import org.apache.spark.sql.{DataFrame, Row}

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

  override def doLogCleanup(dryRun: Boolean = false): DataFrame = {
    val sparkSession = spark
    import sparkSession.implicits._
    val pathColName = "path"

    // with dryRun enabled we just show the logs to be deleted
    if (dryRun) {
      val fileCutOffTime = truncateDay(clock.getTimeMillis() - deltaRetentionMillis).getTime
      val expiredLogsIterator = listExpiredDeltaLogs(fileCutOffTime.getTime)
      val expiredLogPaths = expiredLogsIterator.map(_.getPath.toString).toList
      sparkSession.sparkContext.parallelize(expiredLogPaths).toDF(pathColName)
    }

    // otherwise we do not show logs and delete them if the cleanup is enabled
    else {
      if (enableExpiredLogCleanup) {
        cleanUpExpiredLogs()
      }
      sparkSession.emptyDataFrame
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

  /**
   * Returns an iterator of expired delta logs that can be cleaned up. For a delta log to be
   * considered as expired, it must:
   *  - have a checkpoint file after it
   *  - be older than `fileCutOffTime`
   */
  private def listExpiredDeltaLogs(fileCutOffTime: Long): Iterator[FileStatus] = {
    import org.apache.spark.sql.delta.util.FileNames._

    val latestCheckpoint = lastCheckpoint
    if (latestCheckpoint.isEmpty) return Iterator.empty
    val threshold = latestCheckpoint.get.version - 1L
    val files = store.listFrom(checkpointPrefix(logPath, 0))
      .filter(f => isCheckpointFile(f.getPath) || isDeltaFile(f.getPath))
    def getVersion(filePath: Path): Long = {
      if (isCheckpointFile(filePath)) {
        checkpointVersion(filePath)
      } else {
        deltaVersion(filePath)
      }
    }

    new BufferingLogDeletionIterator(files, fileCutOffTime, threshold, getVersion)
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
