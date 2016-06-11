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

import java.text.MessageFormat;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;

import com.asakusafw.runtime.value.DateUtil;
import com.asakusafw.thundergate.runtime.cache.ThunderGateCacheSupport;

/**
 * Utilities about cache invalidation.
 * @since 0.8.1
 */
public final class Invalidation {

    static final Log LOG = LogFactory.getLog(Invalidation.class);

    /**
     * The Hadoop configuration key of invalidation target tables (in regular expression).
     */
    public static final String KEY_INVALIDATION_TARGET = "com.asakusafw.thundergate.cache.invalidate.tables";

    /**
     * The Hadoop configuration key of invalidation ending timestamp.
     */
    public static final String KEY_INVALIDATION_TIMESTAMP = "com.asakusafw.thundergate.cache.invalidate.until";

    /**
     * The format of timestamp value.
     */
    public static final String FORMAT_TIMESTAMP = "yyyy-MM-dd HH:mm:ss";

    private static final String KEY_INTERNAL_TIMESTAMP = "com.asakusafw.thundergate.cache.invalidate.timestamp";

    private Invalidation() {
        return;
    }

    /**
     * Initializes invalidation timestamp.
     * @param configuration the target configuration
     * @param tableName the target table name
     */
    public static void setupInvalidationTimestamp(Configuration configuration, String tableName) {
        long timestamp = getTimestamp(configuration);
        if (timestamp > 0L && isTarget(configuration, tableName)) {
            LOG.info(MessageFormat.format(
                    "enabling ThunderGate cache invalidation: {0} until {1}",
                    tableName, configuration.get(KEY_INVALIDATION_TIMESTAMP)));
            configuration.setLong(KEY_INTERNAL_TIMESTAMP, timestamp);
        }
    }

    private static long getTimestamp(Configuration conf) {
        String until = conf.get(KEY_INVALIDATION_TIMESTAMP);
        if (until == null) {
            LOG.debug(MessageFormat.format(
                    "invalidation timstamp is not set: {0}",
                    KEY_INVALIDATION_TIMESTAMP));
            return 0L;
        }
        LOG.debug(MessageFormat.format(
                "invalidation timstamp: {0}={1}",
                KEY_INVALIDATION_TIMESTAMP, until));
        long timestamp = DateUtil.parseDateTime(until, '-', ' ', ':');
        if (timestamp < 0) {
            throw new IllegalArgumentException(MessageFormat.format(
                    "invalid timestamp: {0}={1}",
                    KEY_INVALIDATION_TIMESTAMP, until));
        }
        return timestamp;
    }

    private static boolean isTarget(Configuration conf, String table) {
        String pattern = conf.get(KEY_INVALIDATION_TARGET);
        if (pattern == null) {
            LOG.debug(MessageFormat.format(
                    "invalidation target is not set: {0}",
                    KEY_INVALIDATION_TARGET));
            return false;
        }
        try {
            boolean matched = Pattern.compile(pattern).matcher(table).matches();
            LOG.debug(MessageFormat.format(
                    "invalidation target matching: {0}=\"{1}\" / \"{2}\" => {3}",
                    KEY_INVALIDATION_TARGET, pattern, table, matched));
            return matched;
        } catch (PatternSyntaxException e) {
            throw new IllegalArgumentException(MessageFormat.format(
                    "invalid table name pattern: {0}={1}",
                    KEY_INVALIDATION_TARGET, pattern));
        }
    }

    /**
     * Returns the ending timestamp of each record is invalidate.
     * @param configuration the current configuration
     * @return the timestamp - each record is invalidated only if it is older than the timestamp,
     *     may be {@code 0} if invalidation is not enabled
     */
    public static long getInvalidationTimestamp(Configuration configuration) {
        return configuration.getLong(KEY_INTERNAL_TIMESTAMP, 0);
    }

    /**
     * Returns whether the target record is still valid or not.
     * @param model the target record
     * @param invalidation the invalidation timestamp
     * @return {@code true} if it is still valid, otherwise {@code false}
     */
    public static boolean isStillValid(ThunderGateCacheSupport model, long invalidation) {
        return model.__tgc__Timestamp() >= invalidation;
    }
}
