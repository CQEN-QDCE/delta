/*
 * Copyright (2024) The Delta Lake Project Authors.
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
package io.delta.kernel.internal;

import java.sql.Timestamp;

import io.delta.kernel.exceptions.KernelException;

/**
 * Contains methods to create user-facing Delta exceptions.
 */
public final class DeltaErrors {
    private DeltaErrors() {}

    public static KernelException versionBeforeFirstAvailableCommit(
            String tablePath, long versionToLoad, long earliestVersion) {
        String message = String.format(
            "%s: Cannot load table version %s as the transaction log has been truncated due to " +
                "manual deletion or the log/checkpoint retention policy. The earliest available " +
                "version is %s.",
            tablePath,
            versionToLoad,
            earliestVersion);
        return new KernelException(message);
    }

    public static KernelException versionAfterLatestCommit(
            String tablePath, long versionToLoad, long latestVersion) {
        String message = String.format(
            "%s: Cannot load table version %s as it does not exist. " +
                "The latest available version is %s.",
            tablePath,
            versionToLoad,
            latestVersion);
        return new KernelException(message);
    }

    public static KernelException timestampBeforeFirstAvailableCommit(
            String tablePath,
            long providedTimestamp,
            long earliestCommitTimestamp,
            long earliestCommitVersion) {
        String message = String.format(
            "%s: The provided timestamp %s ms (%s) is before the earliest available version %s. " +
                "Please use a timestamp greater than or equal to %s ms (%s).",
            tablePath,
            providedTimestamp,
            formatTimestamp(providedTimestamp),
            earliestCommitVersion,
            earliestCommitTimestamp,
            formatTimestamp(earliestCommitTimestamp));
        return new KernelException(message);
    }

    public static KernelException timestampAfterLatestCommit(
            String tablePath,
            long providedTimestamp,
            long latestCommitTimestamp,
            long latestCommitVersion) {
        String message = String.format(
            "%s: The provided timestamp %s ms (%s) is after the latest available version %s. " +
                "Please use a timestamp less than or equal to %s ms (%s).",
            tablePath,
            providedTimestamp,
            formatTimestamp(providedTimestamp),
            latestCommitVersion,
            latestCommitTimestamp,
            formatTimestamp(latestCommitTimestamp));
        return new KernelException(message);
    }

    /* ------------------------ PROTOCOL EXCEPTIONS ----------------------------- */

    public static KernelException unsupportedReaderProtocol(
            String tablePath, int tableReaderVersion) {
        String message = String.format(
            "Unsupported Delta protocol reader version: table `%s` requires reader version %s " +
                "which is unsupported by this version of Delta Kernel.",
            tablePath,
            tableReaderVersion);
        return new KernelException(message);
    }

    public static KernelException unsupportedReaderFeature(
            String tablePath, String readerFeature) {
        String message = String.format(
            "Unsupported Delta reader feature: table `%s` requires reader table feature \"%s\" " +
                "which is unsupported by this version of Delta Kernel.",
            tablePath,
            readerFeature);
        return new KernelException(message);
    }

    public static KernelException unsupportedWriterProtocol(
            String tablePath, int tableWriterVersion) {
        String message = String.format(
            "Unsupported Delta protocol writer version: table `%s` requires writer version %s " +
                "which is unsupported by this version of Delta Kernel.",
            tablePath,
            tableWriterVersion);
        return new KernelException(message);
    }

    public static KernelException unsupportedWriterFeature(
            String tablePath, String writerFeature) {
        String message = String.format(
            "Unsupported Delta writer feature: table `%s` requires writer table feature \"%s\" " +
                "which is unsupported by this version of Delta Kernel.",
            tablePath,
            writerFeature);
        return new KernelException(message);
    }

    public static KernelException columnInvariantsNotSupported() {
        String message = "This version of Delta Kernel does not support writing to tables with " +
            "column invariants present.";
        return new KernelException(message);
    }

    /* ------------------------ HELPER METHODS ----------------------------- */

    private static String formatTimestamp(long millisSinceEpochUTC) {
        return new Timestamp(millisSinceEpochUTC).toInstant().toString();
    }
}
