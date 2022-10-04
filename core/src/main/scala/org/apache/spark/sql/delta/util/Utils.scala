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

package org.apache.spark.sql.delta.util

import scala.util.Random

/**
 * Various utility methods used by Delta.
 */
object Utils {

  /** Measures the time taken by function `f` */
  def timedMs[T](f: => T): (T, Long) = {
    val start = System.currentTimeMillis()
    val res = f
    val duration = System.currentTimeMillis() - start
    (res, duration)
  }

  /** Generates a string created of `randomPrefixLength` alphanumeric characters. */
  def getRandomPrefix(numChars: Int): String = {
    Random.alphanumeric.take(numChars).mkString
  }
}
