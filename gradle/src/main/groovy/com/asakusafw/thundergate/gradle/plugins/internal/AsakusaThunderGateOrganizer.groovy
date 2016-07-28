/*
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
package com.asakusafw.thundergate.gradle.plugins.internal

import org.gradle.api.Project

import com.asakusafw.gradle.plugins.AsakusafwOrganizerProfile
import com.asakusafw.gradle.plugins.internal.AbstractOrganizer
import com.asakusafw.gradle.plugins.internal.PluginUtils
import com.asakusafw.legacy.gradle.plugins.internal.AsakusaLegacyBaseExtension
import com.asakusafw.legacy.gradle.plugins.internal.AsakusaLegacyBasePlugin
import com.asakusafw.thundergate.gradle.plugins.AsakusafwOrganizerThunderGateExtension

/**
 * Processes an {@link AsakusafwOrganizerProfile} for ThunderGate facilities.
 * @since 0.8.0
 */
class AsakusaThunderGateOrganizer extends AbstractOrganizer {

    /**
     * Creates a new instance.
     * @param project the current project
     * @param profile the target profile
     */
    AsakusaThunderGateOrganizer(Project project, AsakusafwOrganizerProfile profile) {
        super(project, profile)
    }

    /**
     * Configures the target profile.
     */
    @Override
    void configureProfile() {
        createConfigurations()
        configureTasks()
        enableTasks()
    }

    private void createConfigurations() {
        createConfigurations('asakusafw', [
             ThunderGateDist : "Contents of Asakusa Framework ThunderGate modules (${profile.name}).",
          ThunderGateCoreLib : "Libraries of Asakusa Framework ThunderGate common modules (${profile.name}).",
              ThunderGateLib : "Libraries of Asakusa Framework ThunderGate modules (${profile.name}).",
        ])
        configuration('asakusafwThunderGateCoreLib').transitive = false
        PluginUtils.afterEvaluate(project) {
            AsakusaLegacyBaseExtension base = AsakusaLegacyBasePlugin.get(project)
            createDependencies('asakusafw', [
                ThunderGateDist : "com.asakusafw:asakusa-thundergate:${base.featureVersion}:dist@jar",
                ThunderGateLib : [
                    "com.asakusafw:asakusa-thundergate:${base.featureVersion}@jar",
                    "commons-configuration:commons-configuration:${base.commonsConfigurationVersion}@jar",
                    "commons-io:commons-io:${base.commonsIoVersion}@jar",
                    "commons-lang:commons-lang:${base.commonsLangVersion}@jar",
                    "commons-logging:commons-logging:${base.commonsLoggingVersion}@jar",
                    "log4j:log4j:${base.log4jVersion}@jar",
                    "mysql:mysql-connector-java:${base.mysqlConnectorJavaVersion}@jar",
                ],
                ThunderGateCoreLib : [
                    "com.asakusafw:asakusa-thundergate-runtime:${base.featureVersion}@jar"
                ],
            ])
        }
    }

    private void configureTasks() {
        createAttachComponentTasks 'attachComponent', [
            ThunderGate : {
                into('.') {
                    extract configuration('asakusafwThunderGateDist')
                    String targetName = profile.thundergate.target
                    if (targetName) {
                        process {
                            rename(/\[\w+\]-jdbc\.properties/, "${targetName}-jdbc.properties")
                        }
                    }
                }
                into('bulkloader/lib') {
                    put configuration('asakusafwThunderGateLib')
                }
                into('core/lib') {
                    put configuration('asakusafwThunderGateCoreLib')
                }
            },
        ]
    }

    private void enableTasks() {
        PluginUtils.afterEvaluate(project) {
            AsakusafwOrganizerThunderGateExtension extension = profile.thundergate
            if (extension.isEnabled()) {
                project.logger.info "Enabling ThunderGate: ${profile.name}"
                task('attachAssemble').dependsOn task('attachComponentThunderGate')
            }
        }
    }
}
