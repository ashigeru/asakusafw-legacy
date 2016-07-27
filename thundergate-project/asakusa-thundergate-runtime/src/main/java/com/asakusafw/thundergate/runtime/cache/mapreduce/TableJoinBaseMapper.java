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
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.ReflectionUtils;

import com.asakusafw.runtime.io.ModelInput;
import com.asakusafw.runtime.stage.resource.StageResourceDriver;
import com.asakusafw.runtime.stage.temporary.TemporaryStorage;
import com.asakusafw.thundergate.runtime.cache.ThunderGateCacheSupport;

/**
 * Patcher with distributed cache.
 * @since 0.8.1
 */
public class TableJoinBaseMapper extends Mapper<
        NullWritable, ThunderGateCacheSupport,
        NullWritable, ThunderGateCacheSupport> {

    static final Log LOG = LogFactory.getLog(TableJoinBaseMapper.class);

    /**
     * The resource key name.
     */
    public static final String RESOURCE_KEY = "patch";

    private Set<LongWritable> conflicts;

    private final LongWritable buffer = new LongWritable();

    private long invalidate;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        this.conflicts = buildConflicts(context, context.getOutputValueClass().asSubclass(Writable.class));
        this.invalidate = Invalidation.getInvalidationTimestamp(context.getConfiguration());
    }

    private static <T extends Writable> Set<LongWritable> buildConflicts(
            Context context, Class<T> dataType) throws IOException {
        Configuration conf = context.getConfiguration();
        List<Path> caches = getPatchPaths(context);
        T buffer = ReflectionUtils.newInstance(dataType, conf);
        Set<LongWritable> results = new HashSet<>();
        for (Path path : caches) {
            try (ModelInput<T> input = TemporaryStorage.openInput(conf, dataType, path)) {
                while (input.readTo(buffer)) {
                    results.add(new LongWritable(((ThunderGateCacheSupport) buffer).__tgc__SystemId()));
                }
            }
        }
        return results;
    }

    private static List<Path> getPatchPaths(Context context) throws IOException {
        try (StageResourceDriver driver = new StageResourceDriver(context)) {
            return driver.findCache(RESOURCE_KEY);
        }
    }

    @Override
    protected void map(
            NullWritable key,
            ThunderGateCacheSupport value,
            Context context) throws IOException, InterruptedException {
        buffer.set(value.__tgc__SystemId());
        if (value.__tgc__Deleted() == false
                && conflicts.contains(buffer) == false
                && Invalidation.isStillValid(value, invalidate)) {
            context.write(key, value);
        }
    }
}
