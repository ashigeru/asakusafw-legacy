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

import org.gradle.util.GradleVersion
import org.junit.Rule
import org.junit.Test
import org.junit.rules.TemporaryFolder
import org.junit.rules.TestName

import com.asakusafw.gradle.plugins.AsakusafwBasePlugin
import com.asakusafw.gradle.plugins.GradleTestkitHelper

/**
 * Tests for cross Gradle versions compatibility.
 */
class AsakusaUpgradeTest {

    /**
     * temporary project directory.
     */
    @Rule
    public final TemporaryFolder projectDir = new TemporaryFolder()

    /**
     * handles running test name.
     */
    @Rule
    public final TestName testName = new TestName()

    /**
     * Test for the system Gradle version.
     */
    @Test
    void system() {
        doUpgrade(GradleVersion.current().version)
    }

    /**
     * Test for {@code 2.14.1} (Asakusa legacy modules {@code 0.9.0+}).
     */
    @Test
    void 'v2.14.1'() {
        doUpgradeFromTestName()
    }

    private void doUpgradeFromTestName() {
        doUpgrade(testName.methodName.replaceFirst('v', ''))
    }

    private void doUpgrade(String version) {
        Set<File> classpath = GradleTestkitHelper.toClasspath(
            AsakusafwBasePlugin,
            'META-INF/gradle-plugins/asakusafw-sdk.properties',
            AsakusafwLegacyPlugin,
            'META-INF/gradle-plugins/asakusafw-legacy.properties')
        String script = GradleTestkitHelper.getSimpleBuildScript(classpath, 'asakusafw-sdk', 'asakusafw-organizer', 'asakusafw-legacy')
        GradleTestkitHelper.runGradle(projectDir.root, version, script, AsakusafwBasePlugin.TASK_UPGRADE)
    }
}
