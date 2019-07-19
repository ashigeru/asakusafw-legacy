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
import org.gradle.api.file.SourceDirectorySet
import org.gradle.testfixtures.ProjectBuilder
import org.junit.Rule
import org.junit.Test
import org.junit.rules.TestRule
import org.junit.runner.Description
import org.junit.runners.model.Statement

import com.asakusafw.gradle.plugins.AsakusafwOrganizerPlugin
import com.asakusafw.gradle.plugins.AsakusafwPluginConvention
import com.asakusafw.gradle.plugins.internal.AsakusaSdk
import com.asakusafw.gradle.plugins.internal.AsakusaSdkPlugin
import com.asakusafw.legacy.gradle.plugins.internal.AsakusaLegacyBasePlugin
import com.asakusafw.thundergate.gradle.plugins.AsakusafwSdkThunderGateExtension
import com.asakusafw.thundergate.gradle.tasks.GenerateThunderGateDataModelTask

/**
 * Test for {@link AsakusaThunderGateSdkPlugin}.
 */
class AsakusaThunderGateSdkPluginTest {

    /**
     * The test initializer.
     */
    @Rule
    public final TestRule initializer = new TestRule() {
        @Override
        Statement apply(Statement stmt, Description desc) {
            project = ProjectBuilder.builder().withName(desc.methodName).build()
            project.apply plugin: AsakusaThunderGateSdkPlugin
            project.asakusafwBase.frameworkVersion = null
            project.group = 'com.example.testing'
            return stmt
        }
    }

    Project project

    /**
     * test for parent plug-ins.
     */
    @Test
    public void parents() {
        assert project.plugins.hasPlugin(AsakusaSdkPlugin)
        assert !project.plugins.hasPlugin(AsakusafwOrganizerPlugin)
        assert project.plugins.hasPlugin(AsakusaLegacyBasePlugin)
    }

    /**
     * Test for {@code project.asakusafw.thundergate} convention default values.
     */
    @Test
    void extension_defaults() {
        AsakusafwSdkThunderGateExtension extension = project.asakusafw.thundergate
        assert extension.ddlEncoding == null
        assert extension.ddlOutputDirectory == "${project.buildDir}/thundergate/sql"
        assert extension.ddlSourceDirectory == "src/${project.sourceSets.main.name}/sql/modelgen"
        assert extension.deleteColumn == 'DELETE_FLAG'
        assert extension.deleteValue == '"1"'
        assert extension.dmdlOutputDirectory == "${project.buildDir}/thundergate/dmdl"
        assert extension.excludes == null
        assert extension.includes == null
        assert extension.sidColumn == 'SID'
        assert extension.target == null
        assert extension.timestampColumn == 'UPDT_DATETIME'
        assert extension.jdbcFile == null
    }

    /**
     * Test for {@code project.sourceSets.main.thundergateDdl}.
     */
    @Test
    void sourceSets_thundergate() {
        AsakusafwSdkThunderGateExtension extension = project.asakusafw.thundergate
        SourceDirectorySet dirs = project.sourceSets.main.thundergateDdl

        extension.ddlSourceDirectory 'src/main/testing'

        assert dirs.srcDirs.contains(project.file(extension.ddlSourceDirectory))
    }

    /**
     * Test for {@code project.tasks.generateThunderGateDataModel}.
     */
    @Test
    void tasks_generateThunderGateDataModel() {
        def home = setHome('testing/framework')
        AsakusafwPluginConvention convention = project.asakusafw
        convention.logbackConf 'testing/logback'
        convention.maxHeapSize '1G'

        convention.dmdl.dmdlEncoding 'ASCII'
        convention.thundergate.target 'testing'
        convention.thundergate.dmdlOutputDirectory 'testing/dmdlout'
        convention.thundergate.includes 'IN'
        convention.thundergate.excludes 'EX'
        convention.thundergate.sidColumn 'ID'
        convention.thundergate.timestampColumn 'TS'
        convention.thundergate.deleteColumn 'DEL'
        convention.thundergate.deleteValue '"D"'
        convention.thundergate.ddlOutputDirectory 'testing/ddlout'

        GenerateThunderGateDataModelTask task = project.tasks.generateThunderGateDataModel
        assert task.logbackConf == project.file(convention.logbackConf)
        assert task.maxHeapSize == convention.maxHeapSize
        assert task.sourcepath.contains(project.sourceSets.main.thundergateDdl)
        assert task.systemProperties.isEmpty()
        assert task.jvmArgs.isEmpty()

        assert task.ddlEncoding == convention.thundergate.ddlEncoding
        assert task.jdbcConfiguration == new File(home, "bulkloader/conf/${convention.thundergate.target}-jdbc.properties")
        assert task.dmdlOutputDirectory == project.file(convention.thundergate.dmdlOutputDirectory)
        assert task.dmdlOutputEncoding == convention.dmdl.dmdlEncoding
        assert task.includePattern == convention.thundergate.includes
        assert task.excludePattern == convention.thundergate.excludes
        assert task.sidColumnName == convention.thundergate.sidColumn
        assert task.timestampColumnName == convention.thundergate.timestampColumn
        assert task.deleteFlagColumnName == convention.thundergate.deleteColumn
        assert task.deleteFlagColumnValue == convention.thundergate.deleteValue
        assert task.recordLockDdlOutput.canonicalPath.startsWith(project.file(convention.thundergate.ddlOutputDirectory).canonicalPath)
        assert project.files(task.systemDdlFiles).contains(new File(home, 'bulkloader/sql/create_table.sql'))
        assert project.files(task.systemDdlFiles).contains(new File(home, 'bulkloader/sql/insert_import_table_lock.sql'))
    }

    /**
     * Test for {@code project.tasks.generateThunderGateDataModel}.
     */
    @Test
    void tasks_generateThunderGateDataModel_jdbcFile() {
        def home = setHome('testing/framework')
        def jdbc = project.file('testing/jdbc.properties')
        jdbc.text = '''
            jdbc.driver = com.mysql.jdbc.Driver
            jdbc.url = jdbc:mysql://localhost/asakusa
            jdbc.user = asakusa
            jdbc.password = asakusa
            database.name = asakusa
            db.parameter=
            '''.stripIndent()

        AsakusafwPluginConvention convention = project.asakusafw
        convention.thundergate.jdbcFile project.relativePath(jdbc)

        GenerateThunderGateDataModelTask task = project.tasks.generateThunderGateDataModel
        assert task.jdbcConfiguration == jdbc
    }

    private File setHome(Object path) {
        File home = project.file(path)
        home.mkdirs()
        AsakusaSdk.setFrameworkInstallationPath(project, home)
        return home
    }
}
