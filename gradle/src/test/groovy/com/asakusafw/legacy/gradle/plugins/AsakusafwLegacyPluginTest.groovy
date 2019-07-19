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
package com.asakusafw.legacy.gradle.plugins

import org.gradle.api.Project
import org.gradle.testfixtures.ProjectBuilder
import org.junit.Rule
import org.junit.Test
import org.junit.rules.TestRule
import org.junit.runner.Description
import org.junit.runners.model.Statement

import com.asakusafw.legacy.gradle.plugins.internal.AsakusaLegacyOrganizerPlugin
import com.asakusafw.thundergate.gradle.plugins.internal.AsakusaThunderGateOrganizerPlugin
import com.asakusafw.thundergate.gradle.plugins.internal.AsakusaThunderGateSdkPlugin

/**
 * Test for {@link AsakusafwLegacyPlugin}.
 */
class AsakusafwLegacyPluginTest {

    /**
     * The test initializer.
     */
    @Rule
    public final TestRule initializer = new TestRule() {
        @Override
        Statement apply(Statement stmt, Description desc) {
            project = ProjectBuilder.builder().withName(desc.methodName).build()
            project.apply plugin: 'asakusafw-legacy'
            return stmt
        }
    }

    Project project

    /**
     * test for sub plug-ins.
     */
    @Test
    void plugin_compiler() {
        assert !project.plugins.hasPlugin(AsakusaLegacyOrganizerPlugin)
        assert !project.plugins.hasPlugin(AsakusaThunderGateSdkPlugin)
        assert !project.plugins.hasPlugin(AsakusaThunderGateOrganizerPlugin)

        project.apply plugin: 'asakusafw-sdk'
        assert !project.plugins.hasPlugin(AsakusaLegacyOrganizerPlugin)
        assert project.plugins.hasPlugin(AsakusaThunderGateSdkPlugin)
        assert !project.plugins.hasPlugin(AsakusaThunderGateOrganizerPlugin)
    }

    /**
     * test for sub plug-ins.
     */
    @Test
    void plugin_organizer() {
        assert !project.plugins.hasPlugin(AsakusaLegacyOrganizerPlugin)
        assert !project.plugins.hasPlugin(AsakusaThunderGateSdkPlugin)
        assert !project.plugins.hasPlugin(AsakusaThunderGateOrganizerPlugin)

        project.apply plugin: 'asakusafw-organizer'
        assert project.plugins.hasPlugin(AsakusaLegacyOrganizerPlugin)
        assert !project.plugins.hasPlugin(AsakusaThunderGateSdkPlugin)
        assert project.plugins.hasPlugin(AsakusaThunderGateOrganizerPlugin)
    }
}
