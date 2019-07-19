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

import java.text.MessageFormat;
import java.util.Map;

import com.asakusafw.compiler.flow.FlowCompilerOptions;
import com.asakusafw.compiler.flow.Location;
import com.asakusafw.compiler.testing.JobflowInfo;
import com.asakusafw.runtime.stage.StageConstants;

/**
 * Utilities for legacy test drivers.
 * @since 0.8.0
 * @deprecated legacy API
 */
@Deprecated
final class LegacyUtil {

    private LegacyUtil() {
        return;
    }

    /**
     * Computes the canonical input location (for Hadoop) from its name.
     * @param driverContext the current context
     * @param name the input name
     * @return the canonical location of the target input
     */
    public static Location createInputLocation(TestDriverContext driverContext, String name) {
        Location location = Location.fromPath(driverContext.getClusterWorkDir(), '/')
                .append(StageConstants.EXPR_EXECUTION_ID)
                .append("input") //$NON-NLS-1$
                .append(normalize(name));
        return location;
    }

    /**
     * Computes the canonical output location (for Hadoop) from its name.
     * @param driverContext the current context
     * @param name the input name
     * @return the canonical location of the target output
     */
    public static Location createOutputLocation(TestDriverContext driverContext, String name) {
        Location location = Location.fromPath(driverContext.getClusterWorkDir(), '/')
                .append(StageConstants.EXPR_EXECUTION_ID)
                .append("output") //$NON-NLS-1$
                .append(normalize(name))
                .asPrefix();
        return location;
    }

    /**
     * Computes the canonical temporary location (for Hadoop) from the current context.
     * @param driverContext the current context
     * @return the canonical location of the temporary working area
     */
    public static Location createWorkingLocation(TestDriverContext driverContext) {
        Location location = Location.fromPath(driverContext.getClusterWorkDir(), '/')
                .append(StageConstants.EXPR_EXECUTION_ID)
                .append("temp"); //$NON-NLS-1$
        return location;
    }

    /**
     * Returns the compiler options.
     * @param context the current context
     * @return the compiler options
     */
    public static FlowCompilerOptions toCompilerOptions(TestDriverContext context) {
        FlowCompilerOptions options = new FlowCompilerOptions();
        switch (context.getCompilerOptimizeLevel()) {
        case DISABLED:
            options.setCompressConcurrentStage(false);
            options.setCompressFlowPart(false);
            options.setHashJoinForSmall(false);
            options.setHashJoinForTiny(false);
            options.setEnableCombiner(false);
            break;
        case NORMAL:
            options.setCompressConcurrentStage(FlowCompilerOptions.Item.compressConcurrentStage.defaultValue);
            options.setCompressFlowPart(FlowCompilerOptions.Item.compressFlowPart.defaultValue);
            options.setHashJoinForSmall(FlowCompilerOptions.Item.hashJoinForSmall.defaultValue);
            options.setHashJoinForTiny(FlowCompilerOptions.Item.hashJoinForTiny.defaultValue);
            options.setEnableCombiner(FlowCompilerOptions.Item.enableCombiner.defaultValue);
            break;
        case AGGRESSIVE:
            options.setCompressConcurrentStage(true);
            options.setCompressFlowPart(true);
            options.setHashJoinForSmall(true);
            options.setHashJoinForTiny(true);
            options.setEnableCombiner(true);
            break;
        default:
            throw new AssertionError(context.getCompilerOptimizeLevel());
        }
        switch (context.getCompilerDebugLevel()) {
        case DISABLED:
            options.setEnableDebugLogging(false);
            break;
        case NORMAL:
        case VERBOSE:
            options.setEnableDebugLogging(true);
            break;
        default:
            throw new AssertionError(context.getCompilerDebugLevel());
        }
        for (Map.Entry<String, String> entry : context.getCompilerOptions().entrySet()) {
            String name = entry.getKey();
            String value = entry.getValue();
            options.putExtraAttribute(name, value);
        }
        return options;
    }

    public static void prepareIds(TestDriverContext context, JobflowInfo info) {
        context.setCurrentBatchId(info.getJobflow().getBatchId());
        context.setCurrentFlowId(info.getJobflow().getBatchId());
        context.setCurrentExecutionId(MessageFormat.format(
                "{0}-{1}-{2}", //$NON-NLS-1$
                context.getCallerClass().getSimpleName(),
                info.getJobflow().getBatchId(),
                info.getJobflow().getFlowId()));
    }

    private static String normalize(String name) {
        // Hadoop MultipleInputs/Outputs only can accept alphameric characters
        StringBuilder buf = new StringBuilder();
        for (char c : name.toCharArray()) {
            // we use '0' as escape symbol
            if ('1' <= c && c <= '9' || 'A' <= c && c <= 'Z' || 'a' <= c && c <= 'z') {
                buf.append(c);
            } else if (c <= 0xff) {
                buf.append('0');
                buf.append(String.format("%02x", (int) c)); //$NON-NLS-1$
            } else {
                buf.append("0u"); //$NON-NLS-1$
                buf.append(String.format("%04x", (int) c)); //$NON-NLS-1$
            }
        }
        return buf.toString();
    }
}
