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

import java.math.BigDecimal

import org.scalatest.funsuite.AnyFunSuite

import io.delta.golden.GoldenTableUtils.goldenTablePath

import io.delta.kernel.Table

class DeltaTableReadsSuite extends AnyFunSuite with TestUtils {

  for (tablePath <- Seq("basic-decimal-table", "basic-decimal-table-legacy")) {
    test(s"end to end: reading $tablePath") {
      val expectedResult = Seq(
        ("234.00000", "1.00", "2.00000", "3.0000000000"),
        ("2342222.23454", "111.11", "22222.22222", "3333333333.3333333333"),
        ("0.00004", "0.00", "0.00000", "0E-10"),
        ("-2342342.23423", "-999.99", "-99999.99999", "-9999999999.9999999999")
      ).map { tup =>
        (new BigDecimal(tup._1), new BigDecimal(tup._2), new BigDecimal(tup._3),
          new BigDecimal(tup._4))
      }.toSet

      // kernel expects a fully qualified path
      val path = "file:" + goldenTablePath(tablePath)
      val snapshot = Table.forPath(path).getLatestSnapshot(defaultTableClient)

      val result = readSnapshot(snapshot).map { row =>
        (row.getDecimal(0), row.getDecimal(1), row.getDecimal(2), row.getDecimal(3))
      }

      assert(expectedResult == result.toSet)
    }
  }
}
