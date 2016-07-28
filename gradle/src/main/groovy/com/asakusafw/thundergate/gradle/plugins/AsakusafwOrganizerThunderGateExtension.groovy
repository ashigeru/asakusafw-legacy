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
 * An extension object for {@code asakusafwOrgnizer.thundergate}.
 * @since 0.8.0
 */
class AsakusafwOrganizerThunderGateExtension {

    /**
     * Configuration whether ThunderGate features are enabled or not.
     * ThunderGate facilities will be enabled only if this value is {@code true}.
     * <dl>
     *   <dt> Default value: </dt>
     *     <dd> {@code false} </dd>
     * </dl>
     */
    boolean enabled

    /**
     * The ThunderGate default target file name prefix (optional).
     * If this property is set, Asakusa Framework assembly file will include
     * {@code bulkloader/conf/[target]-jdbc.properties} file with following system default settings:
     * <table border="1">
     *   <tr>
     *     <th> Key </th>
     *     <th> Value </th>
     *   </tr>
     *   <tr>
     *     <td> {@code jdbc.driver} </td>
     *     <td> {@code com.mysql.jdbc.Driver} </td>
     *   </tr>
     *   <tr>
     *     <td> {@code jdbc.url} </td>
     *     <td> {@code jdbc:mysql://localhost/asakusa} </td>
     *   </tr>
     *   <tr>
     *     <td> {@code jdbc.user} </td>
     *     <td> {@code asakusa} </td>
     *   </tr>
     *   <tr>
     *     <td> {@code jdbc.password} </td>
     *     <td> {@code asakusa} </td>
     *   </tr>
     *   <tr>
     *     <td> {@code database.name} </td>
     *     <td> {@code asakusa} </td>
     *   </tr>
     *   <tr>
     *     <td> {@code db.parameter} </td>
     *     <td> (empty) </td>
     *   </tr>
     * </table>
     *
     * This configuration may be convenient for development environments with using the above connection settings,
     * but clients should prepare configured settings files and
     * deploy them using {@link AsakusafwOrganizerProfile#assembly} instead in many cases.
     *
     * <dl>
     *   <dt> Default value: </dt>
     *     <dd> {@code null} </dd>
     * </dl>
     */
    String target

    /**
     * Sets the ThunderGate default target file name prefix.
     * @param value the target name
     */
    void setTarget(String value) {
        this.target = value
        if (value != null && !isEnabled()) {
            setEnabled(true)
        }
    }
}
