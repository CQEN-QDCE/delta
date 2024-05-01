/*
 * Copyright (2023) The Delta Lake Project Authors.
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
package io.delta.kernel.defaults.internal.data.vector;

import io.delta.kernel.data.ColumnVector;
import io.delta.kernel.defaults.internal.data.ValueComparator;
import io.delta.kernel.defaults.internal.expressions.VectorComparator;

public class LongValueComparator implements ValueComparator {
    ColumnVector v1;
    ColumnVector v2;
    VectorComparator vectorComparator;

    public LongValueComparator(ColumnVector v1, ColumnVector v2,
                               VectorComparator vectorComparator) {
        this.v1 = v1;
        this.v2 = v2;
        this.vectorComparator = vectorComparator;
    }
    @Override
    public boolean getCompareResult(int rowId) {
        return vectorComparator.compare(Long.compare(v1.getLong(rowId), v2.getLong(rowId)));
    }
}
