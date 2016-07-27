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

import org.gradle.api.NamedDomainObjectCollection
import org.gradle.api.Plugin
import org.gradle.api.Project
import org.gradle.api.Task

import com.asakusafw.gradle.plugins.AsakusafwOrganizerPlugin
import com.asakusafw.gradle.plugins.AsakusafwOrganizerPluginConvention
import com.asakusafw.gradle.plugins.AsakusafwOrganizerProfile
import com.asakusafw.legacy.gradle.plugins.internal.AsakusaLegacyBasePlugin
import com.asakusafw.thundergate.gradle.plugins.AsakusafwOrganizerThunderGateExtension

/**
 * A Gradle sub plug-in for Asakusa organizer about ThunderGate facilities.
 * @since 0.8.0
 */
class AsakusaThunderGateOrganizerPlugin implements Plugin<Project> {

    private Project project

    private NamedDomainObjectCollection<AsakusaThunderGateOrganizer> organizers

    @Override
    void apply(Project project) {
        this.project = project
        this.organizers = project.container(AsakusaThunderGateOrganizer)

        project.apply plugin: 'asakusafw-organizer'
        project.apply plugin: AsakusaLegacyBasePlugin

        configureConvention()
        configureProfiles()
        configureTasks()
    }

    /**
     * Returns the organizers for each profile (only for testing).
     * @return the organizers for each profile
     */
    NamedDomainObjectCollection<AsakusaThunderGateOrganizer> getOrganizers() {
        return organizers
    }

    private void configureConvention() {
        AsakusafwOrganizerPluginConvention convention = project.asakusafwOrganizer
        convention.extensions.create('thundergate', AsakusafwOrganizerThunderGateExtension)
        convention.thundergate.conventionMapping.with {
            enabled = { false }
            target = { null }
        }
    }

    private void configureProfiles() {
        AsakusafwOrganizerPluginConvention convention = project.asakusafwOrganizer
        convention.profiles.all { AsakusafwOrganizerProfile profile ->
            configureProfile(profile)
        }
    }

    private void configureProfile(AsakusafwOrganizerProfile profile) {
        AsakusafwOrganizerPluginConvention convention = project.asakusafwOrganizer
        profile.extensions.create('thundergate', AsakusafwOrganizerThunderGateExtension)
        profile.thundergate.conventionMapping.with {
            enabled = { convention.thundergate.enabled }
            target = { convention.thundergate.target }
        }
        AsakusaThunderGateOrganizer organizer = new AsakusaThunderGateOrganizer(project, profile)
        organizer.configureProfile()
        organizers << organizer
    }

    private void configureTasks() {
        defineFacadeTasks([
              attachComponentThunderGate : 'Attaches ThunderGate components to assemblies.',
        ])
    }

    private void defineFacadeTasks(Map<String, String> taskMap) {
        taskMap.each { String taskName, String desc ->
            project.task(taskName) { Task task ->
                if (desc != null) {
                    task.group AsakusafwOrganizerPlugin.ASAKUSAFW_ORGANIZER_GROUP
                    task.description desc
                }
                organizers.all { AsakusaThunderGateOrganizer organizer ->
                    task.dependsOn organizer.task(task.name)
                }
            }
        }
    }
}
