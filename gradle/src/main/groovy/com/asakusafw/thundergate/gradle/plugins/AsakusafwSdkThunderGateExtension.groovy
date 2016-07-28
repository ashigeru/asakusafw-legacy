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
package com.asakusafw.thundergate.gradle.plugins

/**
 * An extension object for configuring ThunderGate in Asakusa SDK.
 * @since 0.8.0
 */
class AsakusafwSdkThunderGateExtension {

    /**
     * The ThunderGate default name using in the development environment (optional).
     * This will be used for detecting JDBC connection configuration file in the installed Asakusa Framework
     * and generating DMDL files from the target database metadata.
     * ThunderGate facilities will be enabled when this value is non-null,
     * and the facilities may require that the Asakusa Framework with ThunderGate is
     * correctly installed to execute relative tasks.
     * If {@link #jdbcFile} is also set, this property will be ignored.
     * <dl>
     *   <dt> Default value: </dt>
     *     <dd> {@code null} </dd>
     * </dl>
     */
    String target

    /**
     * The external ThunderGate JDBC connection configuration file path (optional).
     * ThunderGate facilities will be enabled when this value is non-null.
     * The target file must be a Java properties file, and it must include following properties:
     * <table>
     *   <tr>
     *     <th> Key </th>
     *     <th> Value </th>
     *   </tr>
     *   <tr>
     *     <th> {@code jdbc.driver} </th>
     *     <th> JDBC driver class name </th>
     *   </tr>
     *   <tr>
     *     <th> {@code jdbc.url} </th>
     *     <th> target database URL </th>
     *   </tr>
     *   <tr>
     *     <th> {@code jdbc.user} </th>
     *     <th> connection user name </th>
     *   </tr>
     *   <tr>
     *     <th> {@code jdbc.password} </th>
     *     <th> connection password </th>
     *   </tr>
     *   <tr>
     *     <th> {@code database.name} </th>
     *     <th> target database name </th>
     *   </tr>
     * </table>
     * If this property is set, the related tasks will not require any Asakusa Framework installations.
     * <dl>
     *   <dt> Default value: </dt>
     *     <dd> {@code null} </dd>
     * </dl>
     */
    String jdbcFile

    /**
     * DDL sources charset encoding name (optional).
     * <dl>
     *   <dt> Default value: </dt>
     *     <dd> {@code null} (use default system encoding) </dd>
     * </dl>
     */
    String ddlEncoding

    /**
     * The DDL source path.
     * <dl>
     *   <dt> Default value: </dt>
     *     <dd> <code>"src/${project.sourceSets.main.name}/sql/modelgen"</code> </dd>
     * </dl>
     */
    String ddlSourceDirectory

    /**
     * The inclusion target table/view name pattern in regular expression (optional).
     * <dl>
     *   <dt> Default value: </dt>
     *     <dd> {@code null} (includes all targets) </dd>
     * </dl>
     */
    String includes

    /**
     * The exclusion target table/view name pattern in regular expression (optional).
     * <dl>
     *   <dt> Default value: </dt>
     *     <dd> {@code null} (no exclusion targets) </dd>
     * </dl>
     */
    String excludes

    /**
     * The generated DMDL files output path from table/view definitions using DDLs.
     * <dl>
     *   <dt> Default value: </dt>
     *     <dd> <code>"${project.buildDir}/thundergate/dmdl"</code> </dd>
     * </dl>
     */
    String dmdlOutputDirectory

    /**
     * The generated SQL files output path from table/view definitions using DDLs.
     * <dl>
     *   <dt> Default value: </dt>
     *     <dd> <code>"${project.buildDir}/thundergate/sql"</code> </dd>
     * </dl>
     */
    String ddlOutputDirectory

    /**
     * The system ID column name (optional).
     * <dl>
     *   <dt> Default value: </dt>
     *     <dd> {@code 'SID'} </dd>
     * </dl>
     */
    String sidColumn

    /**
     * The last modified timestamp column name (optional).
     * <dl>
     *   <dt> Default value: </dt>
     *     <dd> {@code 'UPDT_DATETIME'} </dd>
     * </dl>
     */
    String timestampColumn

    /**
     * The logical delete flag column name (optional).
     * <dl>
     *   <dt> Default value: </dt>
     *     <dd> {@code 'DELETE_FLAG'} </dd>
     * </dl>
     */
    String deleteColumn

    /**
     * The logical delete flag value representation in DMDL (optional).
     * Note that the text values must be enclosed with double-quotations like as {@code "<text-value>"}.
     * <dl>
     *   <dt> Default value: </dt>
     *     <dd> {@code '"1"'} </dd>
     * </dl>
     */
    String deleteValue
}
