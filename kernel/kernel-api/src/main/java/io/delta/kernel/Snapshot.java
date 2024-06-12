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

package io.delta.kernel;

import io.delta.kernel.annotation.Evolving;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.types.StructType;
import io.delta.kernel.internal.actions.Metadata;

/**
 * Represents the snapshot of a Delta table.
 *
 * @since 3.0.0
 */
@Evolving
public interface Snapshot {

    /**
     * Get the version of this snapshot in the table.
     *
     * @param engine {@link Engine} instance to use in Delta Kernel.
     * @return version of this snapshot in the Delta table
     */
    long getVersion(Engine engine);

    /**
     * Get the metadata of the table at this snapshot.
     *
     * @return Metadata of the Delta table at this snapshot.
     */
    Metadata getMetadata();

    /**
     * Get the schema of the table at this snapshot.
     *
     * @param engine {@link Engine} instance to use in Delta Kernel.
     * @return Schema of the Delta table at this snapshot.
     */
    StructType getSchema(Engine engine);

    /**
     * Create a scan builder to construct a {@link Scan} to read data from this snapshot.
     *
     * @param engine {@link Engine} instance to use in Delta Kernel.
     * @return an instance of {@link ScanBuilder}
     */
    ScanBuilder getScanBuilder(Engine engine);

    /**
     * Returns the timestamp of the latest commit of this snapshot.
     * For an uninitialized snapshot, this returns -1.
     * When InCommitTimestampTableFeature is enabled, the timestamp
     * is retrieved from the CommitInfo of the latest commit which
     * can result in an IO operation.
     *
     * @param engine {@link Engine} instance to use in Delta Kernel.
     * @return the timestamp of the latest commit of this snapshot
     */
    long getTimestamp(Engine engine);
}
