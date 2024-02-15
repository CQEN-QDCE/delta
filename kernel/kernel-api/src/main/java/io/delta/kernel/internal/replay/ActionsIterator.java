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

package io.delta.kernel.internal.replay;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Optional;

import io.delta.kernel.client.TableClient;
import io.delta.kernel.data.ColumnarBatch;
import io.delta.kernel.types.StructType;
import io.delta.kernel.utils.CloseableIterator;
import io.delta.kernel.utils.FileStatus;

import io.delta.kernel.internal.util.FileNames;
import io.delta.kernel.internal.util.Utils;
import static io.delta.kernel.internal.util.Utils.singletonCloseableIterator;

/**
 * This class takes as input a list of delta files (.json, .checkpoint.parquet) and produces an
 * iterator of (ColumnarBatch, isFromCheckpoint) tuples, where the schema of the
 * ColumnarBatch semantically represents actions (or, a subset of action fields) parsed from
 * the Delta Log.
 * <p>
 * Users must pass in a `readSchema` to select which actions and sub-fields they want to consume.
 */
class ActionsIterator implements CloseableIterator<ActionWrapper> {
    private final TableClient tableClient;

    /**
     * Iterator over the files.
     * <p>
     * Each file will be split (by 1, or more) to yield an iterator of FileDataReadResults.
     */
    private final Iterator<FileStatus> filesIter;

    private final StructType readSchema;

    /**
     * The current (ColumnarBatch, isFromCheckpoint) tuple. Whenever this iterator
     * is exhausted, we will try and fetch the next one from the `filesIter`.
     * <p>
     * If it is ever empty, that means there are no more batches to produce.
     */
    private Optional<CloseableIterator<ActionWrapper>> actionsIter;

    private boolean closed;

    ActionsIterator(
            TableClient tableClient,
            List<FileStatus> files,
            StructType readSchema) {
        this.tableClient = tableClient;
        this.filesIter = files.iterator();
        this.readSchema = readSchema;
        this.actionsIter = Optional.empty();
    }

    @Override
    public boolean hasNext() {
        if (closed) {
            throw new IllegalStateException("Can't call `hasNext` on a closed iterator.");
        }

        tryEnsureNextActionsIterIsReady();

        // By definition of tryEnsureNextActionsIterIsReady, we know that if actionsIter
        // is non-empty then it has a next element

        return actionsIter.isPresent();
    }

    /**
     * @return a tuple of (ColumnarBatch, isFromCheckpoint), where ColumnarBatch conforms
     * to the instance {@link #readSchema}.
     */
    @Override
    public ActionWrapper next() {
        if (closed) {
            throw new IllegalStateException("Can't call `next` on a closed iterator.");
        }

        if (!hasNext()) {
            throw new NoSuchElementException("No next element");
        }

        return actionsIter.get().next();
    }

    @Override
    public void close() throws IOException {
        if (!closed && actionsIter.isPresent()) {
            actionsIter.get().close();
            actionsIter = Optional.empty();
            closed = true;
        }
    }

    /**
     * If the current `actionsIter` has no more elements, this function finds the next
     * non-empty file in `filesIter` and uses it to set `actionsIter`.
     */
    private void tryEnsureNextActionsIterIsReady() {
        if (actionsIter.isPresent()) {
            // This iterator already has a next element, so we can exit early;
            if (actionsIter.get().hasNext()) {
                return;
            }

            // Clean up resources
            Utils.closeCloseables(actionsIter.get());

            // Set this to empty since we don't know if there's a next file yet
            actionsIter = Optional.empty();
        }

        // Search for the next non-empty file and use that iter
        while (filesIter.hasNext()) {
            actionsIter = Optional.of(getNextActionsIter());

            if (actionsIter.get().hasNext()) {
                // It is ready, we are done
                return;
            }

            // It was an empty file. Clean up resources
            Utils.closeCloseables(actionsIter.get());

            // Set this to empty since we don't know if there's a next file yet
            actionsIter = Optional.empty();
        }
    }

    /**
     * Get the next file from `filesIter` (.json or .checkpoint.parquet), contextualize it
     * (allow the connector to split it), and then read it + inject the `isFromCheckpoint`
     * information.
     * <p>
     * Requires that `filesIter.hasNext` is true.
     */
    private CloseableIterator<ActionWrapper> getNextActionsIter() {
        final FileStatus nextFile = filesIter.next();

        // TODO: [#1965] It should be possible to read our JSON and parquet files
        //       many-at-once instead of one at a time.

        try {
            if (nextFile.getPath().endsWith(".json")) {
                final long fileVersion = FileNames.deltaVersion(nextFile.getPath());

                // Read that file
                final CloseableIterator<ColumnarBatch> dataIter =
                    tableClient.getJsonHandler().readJsonFiles(
                        singletonCloseableIterator(nextFile),
                        readSchema,
                        Optional.empty());

                return combine(dataIter, false /* isFromCheckpoint */, fileVersion);
            } else if (nextFile.getPath().endsWith(".parquet")) {
                final long fileVersion = FileNames.checkpointVersion(nextFile.getPath());

                // Read that file
                final CloseableIterator<ColumnarBatch> dataIter =
                    tableClient.getParquetHandler().readParquetFiles(
                        singletonCloseableIterator(nextFile),
                        readSchema,
                        Optional.empty());

                return combine(dataIter, true /* isFromCheckpoint */, fileVersion);
            } else {
                throw new IllegalStateException(
                    String.format("Unexpected log file path: %s", nextFile.getPath())
                );
            }
        } catch (IOException ex) {
            throw new UncheckedIOException(ex);
        }
    }

    /**
     * Take input (iterator<T>, boolean) and produce an iterator<T, boolean>.
     */
    private CloseableIterator<ActionWrapper> combine(
            CloseableIterator<ColumnarBatch> fileReadDataIter,
            boolean isFromCheckpoint,
            long version) {
        return new CloseableIterator<ActionWrapper>() {
            @Override
            public boolean hasNext() {
                return fileReadDataIter.hasNext();
            }

            @Override
            public ActionWrapper next() {
                return new ActionWrapper(fileReadDataIter.next(), isFromCheckpoint, version);
            }

            @Override
            public void close() throws IOException {
                fileReadDataIter.close();
            }
        };
    }
}
