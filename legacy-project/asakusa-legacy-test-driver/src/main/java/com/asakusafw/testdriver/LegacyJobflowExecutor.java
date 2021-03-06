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
package com.asakusafw.testdriver;

import java.io.File;
import java.io.IOException;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.asakusafw.compiler.flow.ExternalIoCommandProvider;
import com.asakusafw.compiler.flow.ExternalIoCommandProvider.CommandContext;
import com.asakusafw.compiler.testing.JobflowInfo;
import com.asakusafw.compiler.testing.StageInfo;
import com.asakusafw.runtime.stage.StageConstants;
import com.asakusafw.runtime.stage.launcher.LauncherOptionsParser;
import com.asakusafw.runtime.stage.optimizer.LibraryCopySuppressionConfigurator;
import com.asakusafw.testdriver.TestExecutionPlan.Command;
import com.asakusafw.testdriver.TestExecutionPlan.Job;
import com.asakusafw.testdriver.TestExecutionPlan.Task;
import com.asakusafw.testdriver.core.DataModelSourceFactory;
import com.asakusafw.testdriver.core.Difference;
import com.asakusafw.testdriver.core.TestModerator;
import com.asakusafw.testdriver.core.VerifyContext;
import com.asakusafw.testdriver.hadoop.ConfigurationFactory;
import com.asakusafw.utils.collections.Maps;
import com.asakusafw.vocabulary.external.ExporterDescription;
import com.asakusafw.vocabulary.external.ImporterDescription;

/**
 * A legacy jobflow execution mechanism.
 * @since 0.8.0
 * @deprecated legacy API
 */
@Deprecated
class LegacyJobflowExecutor {

    static final Logger LOG = LoggerFactory.getLogger(LegacyJobflowExecutor.class);

    private final TestDriverContext context;

    private final TestModerator moderator;

    private final ConfigurationFactory configurations;

    private final JobExecutor jobExecutor;

    /**
     * Creates a new instance.
     * @param context submission context
     * @throws IllegalArgumentException if some parameters were {@code null}
     */
    LegacyJobflowExecutor(TestDriverContext context) {
        if (context == null) {
            throw new IllegalArgumentException("context must not be null"); //$NON-NLS-1$
        }
        this.context = context;
        this.moderator = new TestModerator(context.getRepository(), context);
        this.jobExecutor = context.getJobExecutor();
        this.configurations = ConfigurationFactory.getDefault();
    }

    /**
     * Cleans up the working directory on the DFS.
     * @throws IOException if failed to clean up
     */
    public void cleanWorkingDirectory() throws IOException {
        Configuration conf = configurations.newInstance();
        FileSystem fs = FileSystem.get(conf);
        Path path = new Path(context.getClusterWorkDir());
        Path fullPath = fs.makeQualified(path);
        LOG.debug("start initializing working directory on the testing runtime: {}", fullPath); //$NON-NLS-1$
        boolean deleted = fs.delete(fullPath, true);
        if (deleted) {
            LOG.debug("finish initializing working directory on the testing runtime: {}", fullPath); //$NON-NLS-1$
        } else {
            LOG.debug("failed to initialize working directory on the testing runtime: {}", fullPath); //$NON-NLS-1$
        }
    }

    /**
     * Cleans up target jobflow's input/output.
     * @param info target jobflow
     * @throws IOException if failed to clean up
     * @throws IllegalArgumentException if some parameters were {@code null}
     */
    public void cleanInputOutput(JobflowInfo info) throws IOException {
        if (info == null) {
            throw new IllegalArgumentException("info must not be null"); //$NON-NLS-1$
        }
        if (context.isSkipCleanInput() == false) {
            for (Map.Entry<String, ImporterDescription> entry : info.getImporterMap().entrySet()) {
                LOG.debug("cleaning input: {}", entry.getKey()); //$NON-NLS-1$
                moderator.truncate(entry.getValue());
            }
        } else {
            LOG.info(Messages.getString("JobflowExecutor.infoSkipInitializeInput")); //$NON-NLS-1$
        }

        if (context.isSkipCleanOutput() == false) {
            for (Map.Entry<String, ExporterDescription> entry : info.getExporterMap().entrySet()) {
                LOG.debug("cleaning output: {}", entry.getKey()); //$NON-NLS-1$
                moderator.truncate(entry.getValue());
            }
        } else {
            LOG.info(Messages.getString("JobflowExecutor.infoSkipInitializeOutput")); //$NON-NLS-1$
        }
    }

    /**
     * Cleans up extra resources.
     * @param resources the external resource map
     * @throws IOException if failed to create job processes
     * @throws IllegalArgumentException if some parameters were {@code null}
     * @since 0.7.3
     */
    public void cleanExtraResources(
            Map<? extends ImporterDescription, ? extends DataModelSourceFactory> resources) throws IOException {
        if (resources == null) {
            throw new IllegalArgumentException("resources must not be null"); //$NON-NLS-1$
        }
        if (context.isSkipCleanInput() == false) {
            for (ImporterDescription description : resources.keySet()) {
                LOG.debug("cleaning external resource: {}", description); //$NON-NLS-1$
                moderator.truncate(description);
            }
        } else {
            LOG.info(Messages.getString("JobflowExecutor.infoSkipInitializeExtraResources")); //$NON-NLS-1$
        }
    }

    /**
     * Prepares the target jobflow's inputs.
     * @param info target jobflow
     * @param inputs target inputs
     * @throws IOException if failed to create job processes
     * @throws IllegalStateException if input is not defined in the jobflow
     * @throws IllegalArgumentException if some parameters were {@code null}
     */
    public void prepareInput(
            JobflowInfo info,
            Iterable<? extends DriverInputBase<?>> inputs) throws IOException {
        if (info == null) {
            throw new IllegalArgumentException("info must not be null"); //$NON-NLS-1$
        }
        if (inputs == null) {
            throw new IllegalArgumentException("inputs must not be null"); //$NON-NLS-1$
        }
        if (context.isSkipPrepareInput() == false) {
            for (DriverInputBase<?> input : inputs) {
                DataModelSourceFactory source = input.getSource();
                if (source != null) {
                    String name = input.getName();
                    LOG.debug("preparing input: {} ({})", name, source); //$NON-NLS-1$
                    ImporterDescription description = info.findImporter(name);
                    checkImporter(info, name, description);
                    moderator.prepare(input.getModelType(), description, source);
                }
            }
        } else {
            LOG.info(Messages.getString("JobflowExecutor.infoSkipPrepareInput")); //$NON-NLS-1$
        }
    }

    /**
     * Prepares the target jobflow's output.
     * @param info target jobflow
     * @param outputs target outputs
     * @throws IOException if failed to create job processes
     * @throws IllegalStateException if output is not defined in the jobflow
     * @throws IllegalArgumentException if some parameters were {@code null}
     */
    public void prepareOutput(
            JobflowInfo info,
            Iterable<? extends DriverOutputBase<?>> outputs) throws IOException {
        if (info == null) {
            throw new IllegalArgumentException("info must not be null"); //$NON-NLS-1$
        }
        if (outputs == null) {
            throw new IllegalArgumentException("outputs must not be null"); //$NON-NLS-1$
        }
        if (context.isSkipPrepareOutput() == false) {
            for (DriverOutputBase<?> output : outputs) {
                DataModelSourceFactory source = output.getSource();
                if (source != null) {
                    String name = output.getName();
                    LOG.debug("preparing output: {} ({})", name, source); //$NON-NLS-1$
                    ExporterDescription description = info.findExporter(name);
                    checkExporter(info, name, description);
                    moderator.prepare(output.getModelType(), description, source);
                }
            }
        } else {
            LOG.info(Messages.getString("JobflowExecutor.infoSkipPrepareOutput")); //$NON-NLS-1$
        }
    }

    private void checkImporter(JobflowInfo info, String name, ImporterDescription description) {
        if (description == null) {
            throw new IllegalStateException(MessageFormat.format(
                    Messages.getString("JobflowExecutor.errorMissingInput"), //$NON-NLS-1$
                    name,
                    info.getJobflow().getFlowId()));
        }
    }

    private void checkExporter(JobflowInfo info, String name, ExporterDescription description) {
        if (description == null) {
            throw new IllegalStateException(MessageFormat.format(
                    Messages.getString("JobflowExecutor.errorMissingOutput"), //$NON-NLS-1$
                    name,
                    info.getJobflow().getFlowId()));
        }
    }

    /**
     * Prepares external resources.
     * @param resources the external resource map
     * @throws IOException if failed to prepare external resources
     * @throws IllegalArgumentException if some parameters were {@code null}
     * @since 0.7.3
     */
    public void prepareExternalResources(
            Map<? extends ImporterDescription, ? extends DataModelSourceFactory> resources) throws IOException {
        if (resources == null) {
            throw new IllegalArgumentException("resources must not be null"); //$NON-NLS-1$
        }
        if (context.isSkipPrepareInput() == false) {
            for (Map.Entry<? extends ImporterDescription, ? extends DataModelSourceFactory> entry
                    : resources.entrySet()) {
                ImporterDescription description = entry.getKey();
                DataModelSourceFactory source = entry.getValue();
                LOG.debug("preparing external resource: {} ({})", description, source); //$NON-NLS-1$
                moderator.prepare(description.getModelType(), description, source);
            }
        } else {
            LOG.info(Messages.getString("JobflowExecutor.infoSkipPrepareExtraResource")); //$NON-NLS-1$
        }
    }

    /**
     * Runs the target jobflow.
     * @param info target jobflow
     * @throws IOException if failed to create job processes
     * @throws IllegalArgumentException if some parameters were {@code null}
     */
    public void runJobflow(JobflowInfo info) throws IOException {
        if (info == null) {
            throw new IllegalArgumentException("info must not be null"); //$NON-NLS-1$
        }
        if (context.isSkipRunJobflow() == false) {
            deployApplication(info);
            CommandContext commands = new CommandContext(
                    context.getFrameworkHomePath().getAbsolutePath() + "/", //$NON-NLS-1$
                    context.getExecutionId(),
                    context.getBatchArgs());
            Map<String, String> dPropMap = createHadoopProperties(commands);
            TestExecutionPlan plan = createExecutionPlan(info, commands, dPropMap);
            validatePlan(plan);
            executePlan(plan, info.getPackageFile());
        } else {
            LOG.info(Messages.getString("JobflowExecutor.infoSkipExecute")); //$NON-NLS-1$
        }
    }

    private void deployApplication(JobflowInfo info) throws IOException {
        LOG.debug("Deploying application library: {}", info.getPackageFile()); //$NON-NLS-1$
        File jobflowDest = context.getJobflowPackageLocation(info.getJobflow().getBatchId());
        FileUtils.copyFileToDirectory(info.getPackageFile(), jobflowDest);

        File dependenciesDest = context.getLibrariesPackageLocation(info.getJobflow().getBatchId());
        if (dependenciesDest.exists()) {
            LOG.debug("Cleaning up dependency libraries: {}", dependenciesDest); //$NON-NLS-1$
            FileUtils.deleteDirectory(dependenciesDest);
        }

        File dependencies = context.getLibrariesPath();
        if (dependencies.exists()) {
            LOG.debug("Deplogying dependency libraries: {} -> {}", dependencies, dependenciesDest); //$NON-NLS-1$
            if (dependenciesDest.mkdirs() == false && dependenciesDest.isDirectory() == false) {
                LOG.warn(MessageFormat.format(
                        Messages.getString("JobflowExecutor.warnFailedToCreateDirectory"), //$NON-NLS-1$
                        dependenciesDest.getAbsolutePath()));
            }
            for (File file : dependencies.listFiles()) {
                if (file.isFile() == false) {
                    continue;
                }
                LOG.debug("Copying a library: {} -> {}", file, dependenciesDest); //$NON-NLS-1$
                FileUtils.copyFileToDirectory(file, dependenciesDest);
            }
        }
    }

    private Map<String, String> createHadoopProperties(CommandContext commands) {
        assert commands != null;
        Map<String, String> dPropMap = new HashMap<>();
        dPropMap.put(StageConstants.PROP_USER, context.getOsUser());
        dPropMap.put(StageConstants.PROP_EXECUTION_ID, commands.getExecutionId());
        dPropMap.put(StageConstants.PROP_ASAKUSA_BATCH_ARGS, commands.getVariableList());
        // disables libraries cache
        dPropMap.put(LauncherOptionsParser.KEY_CACHE_ENABLED, String.valueOf(false));
        // suppresses library copying only if is on local mode
        dPropMap.put(LibraryCopySuppressionConfigurator.KEY_ENABLED, String.valueOf(true));
        dPropMap.putAll(context.getExtraConfigurations());
        return dPropMap;
    }

    private TestExecutionPlan createExecutionPlan(
            JobflowInfo info,
            CommandContext commands,
            Map<String, String> properties) {
        assert info != null;
        assert commands != null;
        assert properties != null;
        List<Job> jobs = new ArrayList<>();
        for (StageInfo stage : info.getStages()) {
            jobs.add(new Job(stage.getClassName(), properties));
        }

        List<Command> initializers = new ArrayList<>();
        List<Command> importers = new ArrayList<>();
        List<Command> exporters = new ArrayList<>();
        List<Command> finalizers = new ArrayList<>();

        for (ExternalIoCommandProvider provider : info.getCommandProviders()) {
            initializers.addAll(convert(provider.getInitializeCommand(commands)));
            importers.addAll(convert(provider.getImportCommand(commands)));
            exporters.addAll(convert(provider.getExportCommand(commands)));
            finalizers.addAll(convert(provider.getFinalizeCommand(commands)));
        }

        return new TestExecutionPlan(
                info.getJobflow().getFlowId(),
                commands.getExecutionId(),
                initializers,
                importers,
                jobs,
                exporters,
                finalizers);
    }

    private List<TestExecutionPlan.Command> convert(List<ExternalIoCommandProvider.Command> commands) {
        List<TestExecutionPlan.Command> results = new ArrayList<>();
        for (ExternalIoCommandProvider.Command cmd : commands) {
            results.add(new TestExecutionPlan.Command(
                    cmd.getCommandTokens(),
                    cmd.getModuleName(),
                    cmd.getProfileName(),
                    cmd.getEnvironment()));
        }
        return results;
    }

    private void validatePlan(TestExecutionPlan plan) {
        jobExecutor.validatePlan(plan);
    }

    private void executePlan(TestExecutionPlan plan, File jobflowPackageFile) throws IOException {
        assert plan != null;
        assert jobflowPackageFile != null;
        if (LOG.isDebugEnabled()) {
            LOG.debug("Executing plan: " //$NON-NLS-1$
                    + "home={}, batchId={}, flowId={}, execId={}, args={}, executor={}", new Object[] { //$NON-NLS-1$
                    context.getFrameworkHomePath(),
                    context.getCurrentBatchId(),
                    context.getCurrentFlowId(),
                    context.getCurrentExecutionId(),
                    context.getBatchArgs(),
                    jobExecutor.getClass().getName(),
            });
        }
        try {
            runJobflowTasks(plan.getInitializers());
            runJobflowTasks(plan.getImporters());
            runJobflowTasks(plan.getJobs());
            runJobflowTasks(plan.getExporters());
        } finally {
            runJobflowTasks(plan.getFinalizers());
        }
    }

    private void runJobflowTasks(List<? extends Task> tasks) throws IOException {
        for (TestExecutionPlan.Task task : tasks) {
            switch (task.getTaskKind()) {
            case COMMAND:
                jobExecutor.execute((TestExecutionPlan.Command) task, getEnvironmentVariables());
                break;
            case HADOOP:
                jobExecutor.execute((TestExecutionPlan.Job) task, getEnvironmentVariables());
                break;
            default:
                throw new AssertionError(task);
            }
        }
    }

    private Map<String, String> getEnvironmentVariables() {
        Map<String, String> variables = Maps.from(context.getEnvironmentVariables());
        return variables;
    }

    /**
     * Verifies the jobflow's results.
     * @param info target jobflow
     * @param verifyContext verification context
     * @param outputs output information
     * @throws IOException if failed to verify
     * @throws IllegalStateException if output is not defined in the jobflow
     * @throws IllegalArgumentException if some parameters were {@code null}
     * @throws AssertionError if actual output is different for the exected output
     */
    public void verify(
            JobflowInfo info,
            VerifyContext verifyContext,
            Iterable<? extends DriverOutputBase<?>> outputs) throws IOException {
        if (info == null) {
            throw new IllegalArgumentException("info must not be null"); //$NON-NLS-1$
        }
        if (verifyContext == null) {
            throw new IllegalArgumentException("verifyContext must not be null"); //$NON-NLS-1$
        }
        if (outputs == null) {
            throw new IllegalArgumentException("outputs must not be null"); //$NON-NLS-1$
        }
        if (context.isSkipVerify() == false) {
            StringBuilder sb = new StringBuilder();
            boolean sawError = false;
            for (DriverOutputBase<?> output : outputs) {
                String name = output.getName();
                ExporterDescription description = info.findExporter(name);
                checkExporter(info, name, description);
                if (output.getResultSink() != null) {
                    LOG.debug("saving result output: {} ({})", output.getName(), output.getResultSink()); //$NON-NLS-1$
                    moderator.save(output.getModelType(), description, output.getResultSink());
                }
                if (output.getVerifier() != null) {
                    LOG.debug("verifying result output: {} ({})", name, output.getVerifier()); //$NON-NLS-1$
                    List<Difference> diffList = moderator.inspect(
                            output.getModelType(),
                            description,
                            verifyContext,
                            output.getVerifier());
                    if (diffList.isEmpty() == false) {
                        sawError = true;
                        String message = MessageFormat.format(
                                Messages.getString("JobflowExecutor.messageDifferenceSummary"), //$NON-NLS-1$
                                info.getJobflow().getFlowId(),
                                output.getName(),
                                diffList.size());
                        sb.append(String.format("%s:%n", message)); //$NON-NLS-1$
                        LOG.warn(message);
                        if (output.getDifferenceSink() != null) {
                            LOG.debug("saving output differences: {} ({})",  //$NON-NLS-1$
                                    name, output.getDifferenceSink());
                            moderator.save(output.getModelType(), diffList, output.getDifferenceSink());
                        }
                        for (Difference difference : diffList) {
                            sb.append(String.format("%s: %s%n", //$NON-NLS-1$
                                    output.getModelType().getSimpleName(),
                                    difference));
                        }
                    }
                }
            }
            if (sawError) {
                throw new AssertionError(sb);
            }
        } else {
            LOG.info(Messages.getString("JobflowExecutor.infoSkipVerifyResult")); //$NON-NLS-1$
        }
    }
}
