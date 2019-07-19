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

import org.gradle.api.Plugin
import org.gradle.api.Project
import org.gradle.api.Task
import org.gradle.api.artifacts.Configuration
import org.gradle.api.file.FileVisitDetails
import org.gradle.api.file.RelativePath
import org.gradle.api.file.SourceDirectorySet
import org.gradle.api.tasks.SourceSet

import com.asakusafw.gradle.plugins.AsakusafwPluginConvention
import com.asakusafw.gradle.plugins.internal.AsakusaSdk
import com.asakusafw.gradle.plugins.internal.AsakusaSdkPlugin
import com.asakusafw.gradle.plugins.internal.PluginUtils
import com.asakusafw.legacy.gradle.plugins.internal.AsakusaLegacyBaseExtension
import com.asakusafw.legacy.gradle.plugins.internal.AsakusaLegacyBasePlugin
import com.asakusafw.thundergate.gradle.plugins.AsakusafwSdkThunderGateExtension
import com.asakusafw.thundergate.gradle.tasks.GenerateThunderGateDataModelTask

/**
 * A Gradle sub plug-in for Asakusa SDK about ThunderGate.
 * @since 0.8.0
 */
class AsakusaThunderGateSdkPlugin implements Plugin<Project> {

    private Project project

    private AsakusafwSdkThunderGateExtension extension

    @Override
    void apply(Project project) {
        this.project = project

        project.apply plugin: 'asakusafw-sdk'
        project.apply plugin: AsakusaLegacyBasePlugin
        extension = AsakusaSdkPlugin.get(project).extensions.create('thundergate', AsakusafwSdkThunderGateExtension)

        configureConvention()
        configureConfigurations()
        configureSourceSets()
        configureTasks()
    }

    private void configureConvention() {
        extension.conventionMapping.with {
            target = { null }
            jdbcFile = { null }
            ddlEncoding = { null }
            ddlSourceDirectory = { (String) "src/${project.sourceSets.main.name}/sql/modelgen" }
            includes = { null }
            excludes = { null }
            dmdlOutputDirectory = { (String) "${project.buildDir}/thundergate/dmdl" }
            ddlOutputDirectory = { (String) "${project.buildDir}/thundergate/sql" }
            sidColumn = { 'SID' }
            timestampColumn = { 'UPDT_DATETIME' }
            deleteColumn = { 'DELETE_FLAG' }
            deleteValue = { '"1"' }
        }
    }

    private void configureConfigurations() {
        project.configurations {
            asakusaThunderGateFiles { Configuration conf ->
                conf.description = 'Asakusa ThunderGate system files.'
                conf.transitive = false
            }
        }
        PluginUtils.afterEvaluate(project) {
            AsakusaLegacyBaseExtension base = AsakusaLegacyBasePlugin.get(project)
            project.dependencies {
                compile "com.asakusafw:asakusa-thundergate-vocabulary:${base.featureVersion}"
                testCompile "com.asakusafw:asakusa-thundergate-test-moderator:${base.featureVersion}"
                asakusaDmdlCompiler "com.asakusafw:asakusa-thundergate-dmdl:${base.featureVersion}"
                asakusaThunderGateFiles "com.asakusafw:asakusa-thundergate:${base.featureVersion}:dist@jar"
            }
        }
    }

    private void configureSourceSets() {
        SourceSet container = project.sourceSets.main

        // ThunderGate DDL source set
        SourceDirectorySet sql = AsakusaSdk.createSourceDirectorySet(project, container, 'thundergateDdl', 'ThunderGate DDL scripts')
        sql.filter.include '**/*.sql'
        sql.srcDirs { extension.ddlSourceDirectory }
        // Note: the generated DMDL source files will be added later only if there are actually required
    }

    private void configureTasks() {
        defineGenerateThunderGateDataModelTask()
    }

    private void defineGenerateThunderGateDataModelTask() {
        // FIXME: should refer DMDL plug-in versions from the legacy base extension?
        AsakusafwPluginConvention sdk = AsakusaSdkPlugin.get(project)
        project.task('generateThunderGateDataModel', type: GenerateThunderGateDataModelTask) { GenerateThunderGateDataModelTask task ->
            task.group AsakusaSdkPlugin.ASAKUSAFW_BUILD_GROUP
            task.description 'Executes ThunderGate DDL files and generates their data models.'
            task.sourcepath << project.sourceSets.main.thundergateDdl
            task.toolClasspath << project.sourceSets.main.compileClasspath
            task.systemDdlFiles << { getThunderGateFile(task, 'bulkloader/sql/create_table.sql') }
            task.systemDdlFiles << { getThunderGateFile(task, 'bulkloader/sql/insert_import_table_lock.sql') }
            task.conventionMapping.with {
                logbackConf = { this.findLogbackConf() }
                maxHeapSize = { sdk.maxHeapSize }
                jdbcConfiguration = {
                    if (extension.jdbcFile != null) {
                        return project.file(extension.jdbcFile)
                    } else {
                        return AsakusaSdk.getFrameworkFile(project, "bulkloader/conf/${extension.target}-jdbc.properties")
                    }
                }
                ddlEncoding = { extension.ddlEncoding }
                includePattern = { extension.includes }
                excludePattern = { extension.excludes }
                dmdlOutputDirectory = { project.file(extension.dmdlOutputDirectory) }
                dmdlOutputEncoding = { sdk.dmdl.dmdlEncoding }
                recordLockDdlOutput = { new File(project.file(extension.ddlOutputDirectory), 'record-lock-ddl.sql') }
                sidColumnName = { extension.sidColumn }
                timestampColumnName = { extension.timestampColumn }
                deleteFlagColumnName = { extension.deleteColumn }
                deleteFlagColumnValue = { extension.deleteValue }
            }
            task.onlyIf { extension.target != null || extension.jdbcFile != null }
            task.doFirst {
                if (extension.jdbcFile == null) {
                    AsakusaSdk.checkFrameworkInstalled(project)
                }
            }
        }
        PluginUtils.afterEvaluate(project) {
            Task task = project.tasks.generateThunderGateDataModel
            if (extension.target == null && extension.jdbcFile == null) {
                project.logger.info "Disables task: ${task.name}"
            } else {
                project.logger.info "Enables task: ${task.name} (using ${extension.jdbcFile ?: extension.target})"
                project.tasks.compileDMDL.dependsOn task
                project.sourceSets.main.dmdl.srcDirs { extension.dmdlOutputDirectory }
            }
        }
    }

    private File getThunderGateFile(Task task, String relativePath) {
        // if jdbcFile is not set, then we use the deployed system files on $ASAKUSA_HOME
        if (extension.jdbcFile == null) {
            return AsakusaSdk.getFrameworkFile(project, relativePath)
        }

        // checking ThunderGate system file cache
        if (task.hasProperty('cacheThunderGateFile') == false) {
            task.ext.cacheThunderGateFile = [:]
        } else {
            File cached = task.cacheThunderGateFile.get(relativePath)
            if (cached != null && cached.isFile()) {
                return cached
            }
        }

        // extracting system files into task temporary directory
        File target = new File(task.temporaryDir, relativePath)
        project.delete target
        File distribution = project.configurations.asakusaThunderGateFiles.files.find()
        if (distribution == null) {
            throw new FileNotFoundException("ThunderGate system files are not configured")
        }
        RelativePath source = RelativePath.parse(true, relativePath)
        project.zipTree(distribution).visit { FileVisitDetails f ->
            if (f.relativePath == source) {
                project.logger.info "Preparing file: ${source} -> ${target}"
                f.copyTo target
                f.stopVisiting()
            }
        }

        // is distribution file broken or unsupported system file path?
        if (target.isFile() == false) {
            throw new FileNotFoundException("Missing '${relativePath}' in ThunderGate system files: ${distribution}")
        }
        task.cacheThunderGateFile.put(relativePath, target)
        return target
    }

    // FIXME refactor
    private File findLogbackConf() {
        AsakusafwPluginConvention sdk = AsakusaSdkPlugin.get(project)
        if (sdk.logbackConf) {
            return project.file(sdk.logbackConf)
        } else {
            return null
        }
    }
}
