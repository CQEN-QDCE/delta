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

import org.apache.spark.sql.delta.actions.{AddFile, Metadata, Protocol, TableFeatureProtocolUtils}

import org.apache.spark.sql.catalyst.expressions.Cast
import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.types._

object TypeWidening {

  /**
   * Returns whether the protocol version supports the Type Widening table feature.
   */
  def isSupported(protocol: Protocol): Boolean =
    protocol.isFeatureSupported(TypeWideningTableFeature)

  /**
   * Returns whether Type Widening is enabled on this table version. Checks that Type Widening is
   * supported, which is a pre-requisite for enabling Type Widening, throws an error if
   * not. When Type Widening is enabled, the type of existing columns or fields can be widened
   * using ALTER TABLE CHANGE COLUMN.
   */
  def isEnabled(protocol: Protocol, metadata: Metadata): Boolean = {
    val isEnabled = DeltaConfigs.ENABLE_TYPE_WIDENING.fromMetaData(metadata)
    if (isEnabled && !isSupported(protocol)) {
      throw new IllegalStateException(
        s"Table property '${DeltaConfigs.ENABLE_TYPE_WIDENING.key}' is " +
          s"set on the table but this table version doesn't support table feature " +
          s"'${TableFeatureProtocolUtils.propertyKey(TypeWideningTableFeature)}'.")
    }
    isEnabled
  }

  /**
   * Returns whether the given type change is eligible for widening. This only checks atomic types.
   * It is the responsibility of the caller to recurse into structs, maps and arrays.
   */
  def isTypeChangeSupported(fromType: AtomicType, toType: AtomicType): Boolean =
    (fromType, toType) match {
      case (from, to) if from == to => true
      // All supported type changes below are supposed to be widening, but to be safe, reject any
      // non-widening change upfront.
      case (from, to) if !Cast.canUpCast(from, to) => false
      case (ByteType, ShortType) => true
      case (ByteType | ShortType, IntegerType) => true
      case _ => false
    }

  /**
   * Filter the given list of files to only keep files that were written before the latest type
   * change, if any. These older files contain a column or field with a type that is different than
   * in the current table schema and must be rewritten when dropping the type widening table feature
   * to make the table readable by readers that don't support the feature.
   */
  def filterFilesRequiringRewrite(snapshot: Snapshot, files: Seq[AddFile]): Seq[AddFile] =
     TypeWideningMetadata.getLatestTypeChangeVersion(snapshot.metadata.schema) match {
      case Some(latestVersion) =>
        files.filter(_.defaultRowCommitVersion match {
            case Some(version) => version < latestVersion
            // Files written before the type widening table feature was added to the table don't
            // have a defaultRowCommitVersion. That does mean they were written before the latest
            // type change.
            case None => true
        })
      case None =>
        Seq.empty
    }


  /**
   * Return the number of files that were written before the latest type change and that then
   * contain a column or field with a type that is different from the current able schema.
   */
  def numFilesRequiringRewrite(snapshot: Snapshot): Long = {
    filterFilesRequiringRewrite(snapshot, snapshot.allFiles.collect()).size
  }
}
