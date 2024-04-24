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
package io.delta.kernel.internal.actions;

import io.delta.kernel.types.*;

/**
 * Delta log action representing an `AddFile`
 */
public class AddFile {

    /* We conditionally read this field based on the query filter */
    private static final StructField JSON_STATS_FIELD = new StructField(
        "stats",
        StringType.STRING,
        true /* nullable */
    );

    /**
     * Schema of the {@code add} action in the Delta Log without stats. Used for constructing
     * table snapshot to read data from the table.
     */
    public static final StructType SCHEMA_WITHOUT_STATS = new StructType()
        .add("path", StringType.STRING, false /* nullable */)
        .add("partitionValues",
            new MapType(StringType.STRING, StringType.STRING, true),
            false /* nullable*/)
        .add("size", LongType.LONG, false /* nullable*/)
        .add("modificationTime", LongType.LONG, false /* nullable*/)
        .add("dataChange", BooleanType.BOOLEAN, false /* nullable*/)
        .add("deletionVector", DeletionVectorDescriptor.READ_SCHEMA, true /* nullable */);

    public static final StructType SCHEMA_WITH_STATS = SCHEMA_WITHOUT_STATS
        .add(JSON_STATS_FIELD);

    /**
     * Full schema of the {@code add} action in the Delta Log.
     */
    public static final StructType FULL_SCHEMA = SCHEMA_WITHOUT_STATS
            .add("stats", StringType.STRING, true /* nullable */)
            .add(
                    "tags",
                    new MapType(StringType.STRING, StringType.STRING, true),
                    true /* nullable */);
    // There are more fields which are added when row-id tracking and clustering is enabled.
    // When Kernel starts supporting row-ids and clustering, we should add those fields here.
}
