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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;

import com.asakusafw.runtime.stage.StageInput;
import com.asakusafw.runtime.stage.input.StageInputDriver;
import com.asakusafw.runtime.stage.input.StageInputFormat;
import com.asakusafw.runtime.stage.input.StageInputMapper;
import com.asakusafw.runtime.stage.input.TemporaryInputFormat;
import com.asakusafw.runtime.stage.output.LegacyBridgeOutputCommitter;
import com.asakusafw.runtime.stage.output.TemporaryOutputFormat;
import com.asakusafw.runtime.stage.resource.StageResourceDriver;
import com.asakusafw.thundergate.runtime.cache.CacheStorage;

/**
 * MapReduce job client for applying cache patch.
 *
 * This requires following command line arguments:
 * <ol>
 * <li> subcommand:
 *   <ul>
 *   <li> {@code "create"} - create a new cache head from {@code <directory>/PATCH} </li>
 *   <li> {@code "update"} - update cache head with merging {@code <directory>/HEAD} and {@code <directory>/PATCH} </li>
 *   </ul>
 * </li>
 * <li> path to the cache directory </li>
 * <li> fully qualified data model class name </li>
 * </ol>
 *
 * This accepts following Hadoop properties:
 * <ul>
 * <li> <code>com.asakusafw.thundergate.cache.invalidate.tables=[regex-table-names]</code>
 *   <ul>
 *   <li>
 *       Invalidation target table names in regular expression:
 *       to invalidate historical data in the cache for all tables, please put <code>.+</code> explicitly
 *   </li>
 *   <li> default: <code>N/A</code> (disabled) </li>
 *   </ul>
 * </li>
 * <li> <code>com.asakusafw.thundergate.cache.invalidate.until=[yyyy-MM-dd HH:mm:ss]</code>
 *   <ul>
 *   <li> MAY remove records (exclusive) older than the specified timestamp </li>
 *   <li> default: <code>N/A</code> (disabled) </li>
 *   </ul>
 * </li>
 * <li> <code>com.asakusafw.thundergate.cache.tablejoin.limit=[size-in-bytes]</code>
 *   <ul>
 *   <li>
 *       The maximum patch size (in bytes) to enable distributed hash based join to update the cache:
 *       otherwise this will use sorted-merge join
 *   </li>
 *   <li> default: <code>-1</code> (distributed hash based join is always disabled) </li>
 *   </ul>
 * </li>
 * </ul>
 *
 * @since 0.2.3
 * @version 0.8.1
 */
public class CacheBuildClient extends Configured implements Tool {

    /**
     * Subcommand to create a new cache.
     */
    public static final String SUBCOMMAND_CREATE = "create";

    /**
     * Subcommand to update a cache.
     */
    public static final String SUBCOMMAND_UPDATE = "update";

    private static final String NEXT_DIRECTORY_NAME = "NEXT";

    private static final String ESCAPE_DIRECTORY_NAME = "PREVIOUS";

    static final Log LOG = LogFactory.getLog(CacheBuildClient.class);

    private CacheStorage storage;

    private Class<?> modelClass;

    private String tableName;

    @Override
    public int run(String[] args) throws Exception {
        if (args.length != 4) {
            throw new IllegalArgumentException(MessageFormat.format(
                    "Invalid arguments: {0}",
                    Arrays.toString(args)));
        }
        String subcommand = args[0];
        boolean create;
        if (subcommand.equals(SUBCOMMAND_CREATE)) {
            create = true;
        } else if (subcommand.equals(SUBCOMMAND_UPDATE)) {
            create = false;
        } else {
            throw new IllegalArgumentException(MessageFormat.format(
                    "Invalid arguments (unknown subcommand): {0}",
                    Arrays.toString(args)));
        }

        Path cacheDirectory = new Path(args[1]);
        modelClass = getConf().getClassByName(args[2]);
        tableName = args[3];
        this.storage = new CacheStorage(getConf(), cacheDirectory.toUri());
        try {
            clearNext();
            if (create) {
                create();
            } else if (PatchStrategy.isTableJoin(tableName, storage)) {
                updateTable();
            } else {
                updateMerge();
            }
            switchHead();
        } finally {
            storage.close();
        }
        return 0;
    }

    private void clearNext() throws IOException {
        LOG.info(MessageFormat.format("Cleaning cache output directory: {0}",
                getNextDirectory()));
        storage.getFileSystem().delete(getNextDirectory(), true);
    }

    private void updateMerge() throws IOException, InterruptedException {
        Job job = newJob();

        List<StageInput> inputList = new ArrayList<>();
        inputList.add(new StageInput(
                storage.getHeadContents("*").toString(),
                TemporaryInputFormat.class,
                MergeJoinBaseMapper.class));
        inputList.add(new StageInput(
                storage.getPatchContents("*").toString(),
                TemporaryInputFormat.class,
                MergeJoinPatchMapper.class));
        StageInputDriver.set(job, inputList);
        job.setInputFormatClass(StageInputFormat.class);
        job.setMapperClass(StageInputMapper.class);
        job.setMapOutputKeyClass(PatchApplyKey.class);
        job.setMapOutputValueClass(modelClass);

        // combiner may have no effect in normal cases
        job.setReducerClass(MergeJoinReducer.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(modelClass);
        job.setPartitionerClass(PatchApplyKey.Partitioner.class);
        job.setSortComparatorClass(PatchApplyKey.SortComparator.class);
        job.setGroupingComparatorClass(PatchApplyKey.GroupComparator.class);

        TemporaryOutputFormat.setOutputPath(job, getNextDirectory());
        job.setOutputFormatClass(TemporaryOutputFormat.class);
        job.getConfiguration().setClass(
                "mapred.output.committer.class",
                LegacyBridgeOutputCommitter.class,
                org.apache.hadoop.mapred.OutputCommitter.class);

        LOG.info(MessageFormat.format("applying patch (merge join): {0} / {1} -> {2}",
                storage.getPatchContents("*"),
                storage.getHeadContents("*"),
                getNextContents()));
        try {
            boolean succeed = job.waitForCompletion(true);
            LOG.info(MessageFormat.format("applied patch (merge join): succeed={0}, {1} / {2} -> {3}",
                    succeed,
                    storage.getPatchContents("*"),
                    storage.getHeadContents("*"),
                    getNextContents()));
            if (succeed == false) {
                throw new IOException(MessageFormat.format("failed to apply patch (merge join): {0} / {1} -> {2}",
                        storage.getPatchContents("*"),
                        storage.getHeadContents("*"),
                        getNextContents()));
            }
        } catch (ClassNotFoundException e) {
            throw new IOException(e);
        }
        putMeta();
    }

    private void putMeta() throws IOException {
        LOG.info(MessageFormat.format("setting patched properties: {0} -> {1}",
                storage.getPatchProperties(),
                getNextDirectory()));
        FileUtil.copy(
                storage.getFileSystem(),
                storage.getPatchProperties(),
                storage.getFileSystem(),
                getNextProperties(),
                false,
                storage.getConfiguration());
    }

    private void updateTable() throws IOException, InterruptedException {
        Job job = newJob();
        List<StageInput> inputList = new ArrayList<>();
        inputList.add(new StageInput(
                storage.getHeadContents("*").toString(),
                TemporaryInputFormat.class,
                TableJoinBaseMapper.class));
        inputList.add(new StageInput(
                storage.getPatchContents("*").toString(),
                TemporaryInputFormat.class,
                TableJoinPatchMapper.class));
        StageInputDriver.set(job, inputList);
        StageResourceDriver.add(job, storage.getPatchContents("*").toString(), TableJoinBaseMapper.RESOURCE_KEY);
        job.setInputFormatClass(StageInputFormat.class);
        job.setMapperClass(StageInputMapper.class);
        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(modelClass);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(modelClass);

        TemporaryOutputFormat.setOutputPath(job, getNextDirectory());
        job.setOutputFormatClass(TemporaryOutputFormat.class);
        job.getConfiguration().setClass(
                "mapred.output.committer.class",
                LegacyBridgeOutputCommitter.class,
                org.apache.hadoop.mapred.OutputCommitter.class);

        job.setNumReduceTasks(0);

        LOG.info(MessageFormat.format("applying patch (table join): {0} / {1} -> {2}",
                storage.getPatchContents("*"),
                storage.getHeadContents("*"),
                getNextContents()));
        try {
            boolean succeed = job.waitForCompletion(true);
            LOG.info(MessageFormat.format("applied patch (table join): succeed={0}, {1} / {2} -> {3}",
                    succeed,
                    storage.getPatchContents("*"),
                    storage.getHeadContents("*"),
                    getNextContents()));
            if (succeed == false) {
                throw new IOException(MessageFormat.format("failed to apply patch (table join): {0} / {1} -> {2}",
                        storage.getPatchContents("*"),
                        storage.getHeadContents("*"),
                        getNextContents()));
            }
        } catch (ClassNotFoundException e) {
            throw new IOException(e);
        }
        putMeta();
    }

    private void create() throws InterruptedException, IOException {
        Job job = newJob();
        List<StageInput> inputList = new ArrayList<>();
        inputList.add(new StageInput(
                storage.getPatchContents("*").toString(),
                TemporaryInputFormat.class,
                CreateCacheMapper.class));
        StageInputDriver.set(job, inputList);
        job.setInputFormatClass(StageInputFormat.class);
        job.setMapperClass(StageInputMapper.class);
        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(modelClass);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(modelClass);

        TemporaryOutputFormat.setOutputPath(job, getNextDirectory());
        job.setOutputFormatClass(TemporaryOutputFormat.class);
        job.getConfiguration().setClass(
                "mapred.output.committer.class",
                LegacyBridgeOutputCommitter.class,
                org.apache.hadoop.mapred.OutputCommitter.class);

        job.setNumReduceTasks(0);

        LOG.info(MessageFormat.format("applying patch (no join): {0} / (empty) -> {2}",
                storage.getPatchContents("*"),
                storage.getHeadContents("*"),
                getNextContents()));
        try {
            boolean succeed = job.waitForCompletion(true);
            LOG.info(MessageFormat.format("applied patch (no join): succeed={0}, {1} / (empty) -> {3}",
                    succeed,
                    storage.getPatchContents("*"),
                    storage.getHeadContents("*"),
                    getNextContents()));
            if (succeed == false) {
                throw new IOException(MessageFormat.format("failed to apply patch (no join): {0} / (empty) -> {2}",
                        storage.getPatchContents("*"),
                        storage.getHeadContents("*"),
                        getNextContents()));
            }
        } catch (ClassNotFoundException e) {
            throw new IOException(e);
        }
        putMeta();
    }

    private Job newJob() throws IOException {
        Job job = Job.getInstance(getConf());
        job.setJobName("TGC-CREATE-" + tableName);
        Configuration conf = job.getConfiguration();
        Invalidation.setupInvalidationTimestamp(conf, tableName);
        return job;
    }

    private void switchHead() throws IOException {
        boolean hasHead = storage.getFileSystem().exists(storage.getHeadDirectory());
        if (hasHead) {
            LOG.info(MessageFormat.format(
                    "Escaping previous cache: {0} -> {1}",
                    storage.getHeadDirectory(),
                    getEscapeDir()));
            storage.getFileSystem().delete(getEscapeDir(), true);
            storage.getFileSystem().rename(storage.getHeadDirectory(), getEscapeDir());
        }

        LOG.info(MessageFormat.format(
                "Switching patched as HEAD: {0} -> {1}",
                getNextDirectory(),
                storage.getHeadDirectory()));
        storage.getFileSystem().rename(getNextDirectory(), storage.getHeadDirectory());

        if (hasHead) {
            LOG.info(MessageFormat.format(
                    "Cleaning previous cache: {0}",
                    storage.getHeadDirectory()));
            storage.getFileSystem().delete(getEscapeDir(), true);
        }
    }

    private Path getNextDirectory() {
        return new Path(storage.getTempoaryDirectory(), NEXT_DIRECTORY_NAME);
    }

    private Path getNextProperties() {
        return new Path(getNextDirectory(), CacheStorage.META_FILE_NAME);
    }

    private Path getNextContents() {
        return new Path(getNextDirectory(), CacheStorage.CONTENT_FILE_GLOB);
    }

    private Path getEscapeDir() {
        return new Path(storage.getTempoaryDirectory(), ESCAPE_DIRECTORY_NAME);
    }
}
