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
import org.gradle.testfixtures.ProjectBuilder
import org.junit.Rule
import org.junit.Test
import org.junit.rules.TestRule
import org.junit.runner.Description
import org.junit.runners.model.Statement

import com.asakusafw.gradle.plugins.AsakusafwOrganizerPlugin
import com.asakusafw.gradle.plugins.AsakusafwOrganizerProfile
import com.asakusafw.gradle.plugins.OrganizerTestRoot
import com.asakusafw.gradle.plugins.internal.AsakusaSdkPlugin
import com.asakusafw.legacy.gradle.plugins.internal.AsakusaLegacyBasePlugin
import com.asakusafw.thundergate.gradle.plugins.AsakusafwOrganizerThunderGateExtension

/**
 * Test for {@link AsakusaThunderGateOrganizerPlugin}.
 */
class AsakusaThunderGateOrganizerPluginTest extends OrganizerTestRoot {

    /**
     * The test initializer.
     */
    @Rule
    public final TestRule initializer = new TestRule() {
        @Override
        Statement apply(Statement stmt, Description desc) {
            project = ProjectBuilder.builder().withName(desc.methodName).build()
            project.apply plugin: AsakusaThunderGateOrganizerPlugin
            return stmt
        }
    }

    Project project

    /**
     * test for parent plug-ins.
     */
    @Test
    public void parents() {
        assert !project.plugins.hasPlugin(AsakusaSdkPlugin)
        assert project.plugins.hasPlugin(AsakusafwOrganizerPlugin)
        assert project.plugins.hasPlugin(AsakusaLegacyBasePlugin)
    }

    /**
     * Test for {@code project.asakusafwOrganizer.thundergate} convention default values.
     */
    @Test
    public void extension_defaults() {
        AsakusafwOrganizerThunderGateExtension extension = project.asakusafwOrganizer.thundergate
        assert extension.enabled == false
        assert extension.target == null
    }

    /**
     * Test for {@code project.asakusafwOrganizer.profiles.*.thundergate} default values.
     */
    @Test
    public void profile_defaults() {
        AsakusafwOrganizerThunderGateExtension extension = project.asakusafwOrganizer.thundergate
        AsakusafwOrganizerThunderGateExtension profile = project.asakusafwOrganizer.profiles.testp.thundergate
        assert profile.enabled == extension.enabled
        assert profile.target == extension.target
    }

    /**
     * Test for {@code project.asakusafwOrganizer.thundergate.enable}.
     */
    @Test
    public void thundergate_enable_by_target() {
        AsakusafwOrganizerThunderGateExtension extension = project.asakusafwOrganizer.thundergate
        extension.target 'testing'
        assert extension.enabled
        assert extension.target == 'testing'
    }

    /**
     * Test for {@code project.tasks} for organizer facade tasks.
     */
    @Test
    public void tasks_common() {
        assert project.tasks.attachComponentThunderGate
    }

    /**
     * Test for {@code project.tasks} for profile tasks.
     */
    @Test
    public void tasks_profile() {
        AsakusafwOrganizerProfile profile = project.asakusafwOrganizer.profiles.testp
        assert ptask(profile, 'attachComponentThunderGate')
    }

    /**
     * Test for dependencies of {@code project.tasks} in organizer facade tasks.
     */
    @Test
    public void tasks_dependencies() {
        checkDependencies('attachComponentThunderGate')
    }
}
