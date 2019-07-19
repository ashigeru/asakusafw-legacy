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
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.asakusafw.compiler.testing.JobflowInfo;
import com.asakusafw.testdriver.compiler.basic.BasicJobflowMirror;
import com.asakusafw.testdriver.core.DataModelSourceFactory;
import com.asakusafw.testdriver.core.VerifyContext;
import com.asakusafw.vocabulary.external.ImporterDescription;

@Deprecated
class LegacyJobflowExecutor {

    static final Logger LOG = LoggerFactory.getLogger(LegacyJobflowExecutor.class);

    private final TestDriverContext driverContext;

    private final JobflowExecutor delegate;

    /**
     * Creates a new instance.
     * @param context submission context
     * @throws IllegalArgumentException if some parameters were {@code null}
     */
    LegacyJobflowExecutor(TestDriverContext context) {
        if (context == null) {
            throw new IllegalArgumentException("context must not be null"); //$NON-NLS-1$
        }
        this.driverContext = context;
        this.delegate = new JobflowExecutor(context);
    }

    /**
     * Cleans up the working directory on the DFS.
     * @throws IOException if failed to clean up
     */
    public void cleanWorkingDirectory() throws IOException {
        delegate.cleanWorkingDirectory();
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
        delegate.prepareExternalResources(resources);
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
        if (driverContext.isSkipRunJobflow() == false) {
            deployApplication(info);
            BasicJobflowMirror jobflow = MapReduceCompierUtil.toJobflow(info);
            delegate.validateJobflow(jobflow);
            delegate.runJobflow(jobflow);
        } else {
            LOG.info(Messages.getString("JobflowExecutor.infoSkipExecute")); //$NON-NLS-1$
        }
    }

    private void deployApplication(JobflowInfo info) throws IOException {
        LOG.debug("Deploying application library: {}", info.getPackageFile()); //$NON-NLS-1$
        File jobflowDest = driverContext.getJobflowPackageLocation(info.getJobflow().getBatchId());
        FileUtils.copyFileToDirectory(info.getPackageFile(), jobflowDest);

        File dependenciesDest = driverContext.getLibrariesPackageLocation(info.getJobflow().getBatchId());
        if (dependenciesDest.exists()) {
            LOG.debug("Cleaning up dependency libraries: {}", dependenciesDest); //$NON-NLS-1$
            FileUtils.deleteDirectory(dependenciesDest);
        }

        File dependencies = driverContext.getLibrariesPath();
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
        BasicJobflowMirror jobflow = MapReduceCompierUtil.toJobflow(info);
        delegate.verify(jobflow, verifyContext, outputs);
    }
}
