/**
 * Copyright 2011-2016 Asakusa Framework Team.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.asakusafw.thundergate.runtime.cache.mapreduce;

import java.io.IOException;
import java.text.MessageFormat;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileStatus;

import com.asakusafw.runtime.stage.temporary.TemporaryStorage;
import com.asakusafw.thundergate.runtime.cache.CacheStorage;

/**
 * Utilities about merge strategy.
 * @since 0.8.1
 */
public final class PatchStrategy {

    static final Log LOG = LogFactory.getLog(PatchStrategy.class);

    /**
     * The Hadoop configuration key of maximum patch size of table join strategy.
     */
    public static final String KEY_TABLE_JOIN_LIMIT = "com.asakusafw.thundergate.cache.tablejoin.limit";

    /**
     * The default value of {@link #KEY_TABLE_JOIN_LIMIT}.
     */
    public static final long DEFAULT_TABLE_JOIN_LIMIT = -1L;

    private PatchStrategy() {
        return;
    }

    /**
     * Returns whether or not the table join is enabled for the target cache.
     * @param tableName the table name
     * @param cache the target cache.
     * @return {@code true} if the table join is enabled, otherwise {@code false}
     */
    public static boolean isTableJoin(String tableName, CacheStorage cache) {
        long limit = cache.getConfiguration().getLong(KEY_TABLE_JOIN_LIMIT, DEFAULT_TABLE_JOIN_LIMIT);
        if (limit <= 0) {
            LOG.info(MessageFormat.format(
                    "cache table join is disabled: {0}",
                    tableName));
            return false;
        }
        LOG.info(MessageFormat.format("computing patch content size: {0}",
                tableName));
        try {
            long total = 0;
            for (FileStatus stat : TemporaryStorage.listStatus(
                    cache.getConfiguration(), cache.getPatchContents("*"))) {
                total += stat.getLen();
            }
            LOG.info(MessageFormat.format("patch content size: {1}bytes (table-join-limit={2}, {0})",
                    tableName,
                    total,
                    limit));
            return total <= limit;
        } catch (IOException e) {
            LOG.warn(MessageFormat.format(
                    "failed to compute patch size: {0}",
                    tableName), e);
            return false;
        }
    }
}
