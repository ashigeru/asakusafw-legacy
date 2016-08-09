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
package com.asakusafw.testdriver.bulkloader;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import org.junit.Rule;
import org.junit.Test;

import com.asakusafw.runtime.io.ModelOutput;
import com.asakusafw.testdriver.core.DataModelDefinition;
import com.asakusafw.testdriver.core.DataModelReflection;
import com.asakusafw.testdriver.core.DataModelSource;
import com.asakusafw.testdriver.core.PropertyName;
import com.asakusafw.testdriver.model.SimpleDataModelDefinition;
import com.asakusafw.vocabulary.bulkloader.BulkLoadExporterDescription;
import com.asakusafw.vocabulary.bulkloader.DupCheckDbExporterDescription;

/**
 * Test for {@link BulkLoadExporterRetriever}.
 */
public class BulkLoadExporterRetrieverTest {

    static final DataModelDefinition<Simple> SIMPLE = new SimpleDataModelDefinition<>(Simple.class);

    static final DataModelDefinition<DupCheck> DUP_CHECK = new SimpleDataModelDefinition<>(DupCheck.class);

    static final DataModelDefinition<Union> UNION = new SimpleDataModelDefinition<>(Union.class);

    static final DataModelDefinition<Invalid> INVALID = new SimpleDataModelDefinition<>(Invalid.class);

    static final BulkLoadExporterDescription NORMAL_DESC = new DupCheckDbExporterDescription() {

        @Override
        public Class<?> getModelType() {
            return Simple.class;
        }

        @Override
        public String getTargetName() {
            return "exporter";
        }

        @Override
        protected Class<?> getNormalModelType() {
            return Simple.class;
        }

        @Override
        protected Class<?> getErrorModelType() {
            return DupCheck.class;
        }

        @Override
        protected String getErrorCodeValue() {
            return "aaa";
        }

        @Override
        protected String getErrorCodeColumnName() {
            return "TEXT";
        }

        @Override
        protected List<String> getCheckColumnNames() {
            return Arrays.asList("NUMBER");
        }
    };

    static final BulkLoadExporterDescription UNION_DESC = new DupCheckDbExporterDescription() {

        @Override
        public Class<?> getModelType() {
            return Union.class;
        }

        @Override
        public String getTargetName() {
            return "exporter";
        }

        @Override
        protected Class<?> getNormalModelType() {
            return Simple.class;
        }

        @Override
        protected Class<?> getErrorModelType() {
            return DupCheck.class;
        }

        @Override
        protected String getErrorCodeValue() {
            return "aaa";
        }

        @Override
        protected String getErrorCodeColumnName() {
            return "TEXT";
        }

        @Override
        protected List<String> getCheckColumnNames() {
            return Arrays.asList("NUMBER");
        }
    };

    static final BulkLoadExporterDescription MISSING_DESC = new DupCheckDbExporterDescription() {

        @Override
        public Class<?> getModelType() {
            return Simple.class;
        }

        @Override
        public String getTargetName() {
            return "exporter";
        }

        @Override
        protected Class<?> getNormalModelType() {
            return Simple.class;
        }

        @Override
        protected Class<?> getErrorModelType() {
            return DupCheck.class;
        }

        @Override
        protected String getNormalTableName() {
            return "INVALID_NORMAL";
        }

        @Override
        protected String getErrorTableName() {
            return "INVALID_ERROR";
        }

        @Override
        protected String getErrorCodeValue() {
            return "aaa";
        }

        @Override
        protected String getErrorCodeColumnName() {
            return "TEXT";
        }

        @Override
        protected List<String> getCheckColumnNames() {
            return Arrays.asList("NUMBER");
        }
    };

    /**
     * H2 database.
     */
    @Rule
    public H2Resource h2 = new H2Resource("exporter") {
        @Override
        protected void before() throws Exception {
            executeFile("ddl-simple.sql");
            executeFile("ddl-dupcheck.sql");
        }
    };

    /**
     * Configuration helper.
     */
    @Rule
    public ConfigurationContext context = new ConfigurationContext();

    /**
     * truncate tables.
     * @throws Exception if occur
     */
    @Test
    public void truncate() throws Exception {
        Simple simple = new Simple();
        simple.number = 100;
        simple.text = "Hello, world!";
        insert(simple, SIMPLE, "SIMPLE");

        DupCheck dc = new DupCheck();
        dc.number = 100;
        dc.text = "Hello, world!";
        insert(dc, DUP_CHECK, "DUP_CHECK");

        context.put("exporter", "exporter");
        BulkLoadExporterRetriever exporter = new BulkLoadExporterRetriever();

        assertThat(h2.count("SIMPLE"), is(1));
        assertThat(h2.count("DUP_CHECK"), is(1));
        exporter.truncate(NORMAL_DESC);

        assertThat(h2.count("SIMPLE"), is(0));
        assertThat(h2.count("DUP_CHECK"), is(0));
    }

    /**
     * output to normal.
     * @throws Exception if occur
     */
    @Test
    public void output() throws Exception {
        Simple object = new Simple();
        object.number = 100;
        object.text = "Hello, world!";

        context.put("exporter", "exporter");
        BulkLoadExporterRetriever exporter = new BulkLoadExporterRetriever();
        try (ModelOutput<Simple> output = exporter.createOutput(SIMPLE, NORMAL_DESC)) {
            output.write(object);
        }
        assertThat(h2.count("SIMPLE"), is(1));
        assertThat(h2.count("DUP_CHECK"), is(0));

        List<Simple> list = retrieve(SIMPLE, "SIMPLE");
        assertThat(list, is(Arrays.asList(object)));
    }

    /**
     * output to dupcheck.
     * @throws Exception if occur
     */
    @Test
    public void output_dupcheck() throws Exception {
        DupCheck object = new DupCheck();
        object.number = 100;
        object.text = "Hello, world!";

        context.put("exporter", "exporter");
        BulkLoadExporterRetriever exporter = new BulkLoadExporterRetriever();
        try (ModelOutput<DupCheck> output = exporter.createOutput(DUP_CHECK, NORMAL_DESC)) {
            output.write(object);
        }
        assertThat(h2.count("SIMPLE"), is(0));
        assertThat(h2.count("DUP_CHECK"), is(1));

        List<DupCheck> list = retrieve(DUP_CHECK, "DUP_CHECK");
        assertThat(list, is(Arrays.asList(object)));
    }

    /**
     * attempted to output with invalid type.
     * @throws Exception if occur
     */
    @Test(expected = IOException.class)
    public void output_invalid() throws Exception {
        context.put("exporter", "exporter");
        BulkLoadExporterRetriever exporter = new BulkLoadExporterRetriever();
        try (ModelOutput<?> output = exporter.createOutput(INVALID, NORMAL_DESC)) {
            // do nothing
        }
    }

    /**
     * attempted to output to unknown table.
     * @throws Exception if occur
     */
    @Test(expected = IOException.class)
    public void output_missing() throws Exception {
        context.put("exporter", "exporter");
        BulkLoadExporterRetriever exporter = new BulkLoadExporterRetriever();
        try (ModelOutput<?> output = exporter.createOutput(SIMPLE, MISSING_DESC)) {
            // do nothing
        }
    }

    /**
     * retrieve.
     * @throws Exception if occur
     */
    @Test
    public void source() throws Exception {
        Simple object = new Simple();
        object.number = 100;
        object.text = "Hello, world!";
        insert(object, SIMPLE, "SIMPLE");

        context.put("exporter", "exporter");
        BulkLoadExporterRetriever exporter = new BulkLoadExporterRetriever();
        try (DataModelSource source = exporter.createSource(SIMPLE, NORMAL_DESC)) {
            List<Simple> results = drain(SIMPLE, source);
            assertThat(results, is(Arrays.asList(object)));
        }
    }

    /**
     * retrieve from dupcheck.
     * @throws Exception if occur
     */
    @Test
    public void source_dupcheck() throws Exception {
        DupCheck object = new DupCheck();
        object.number = 100;
        object.text = "Hello, world!";
        insert(object, DUP_CHECK, "DUP_CHECK");

        context.put("exporter", "exporter");
        BulkLoadExporterRetriever exporter = new BulkLoadExporterRetriever();
        try (DataModelSource source = exporter.createSource(DUP_CHECK, NORMAL_DESC)) {
            List<DupCheck> results = drain(DUP_CHECK, source);
            assertThat(results, is(Arrays.asList(object)));
        }
    }

    /**
     * attempt to retrieve with invalid type.
     * @throws Exception if occur
     */
    @Test(expected = IOException.class)
    public void source_invalid() throws Exception {
        context.put("exporter", "exporter");
        BulkLoadExporterRetriever exporter = new BulkLoadExporterRetriever();
        try (DataModelSource source = exporter.createSource(INVALID, NORMAL_DESC)) {
            // do nothing
        }
    }

    /**
     * attempt to retrieve from missing table.
     * @throws Exception if occur
     */
    @Test(expected = IOException.class)
    public void source_missing() throws Exception {
        context.put("exporter", "exporter");
        BulkLoadExporterRetriever exporter = new BulkLoadExporterRetriever();
        try (DataModelSource source = exporter.createSource(SIMPLE, MISSING_DESC)) {
            // do nothing
        }
    }

    /**
     * output to normal w/ union.
     * @throws Exception if occur
     */
    @Test
    public void output_union() throws Exception {
        Union object = new Union();
        object.number = 100;
        object.text = "Hello, world!";

        context.put("exporter", "exporter");
        BulkLoadExporterRetriever exporter = new BulkLoadExporterRetriever();
        try (ModelOutput<Union> output = exporter.createOutput(UNION, UNION_DESC)) {
            output.write(object);
        }
        assertThat(h2.count("SIMPLE"), is(1));
        assertThat(h2.count("DUP_CHECK"), is(0));

        List<Union> list = retrieve(UNION, "SIMPLE");
        assertThat(list, is(Arrays.asList(object)));
    }

    /**
     * output to normal w/ union.
     * @throws Exception if occur
     */
    @Test
    public void output_union_normal() throws Exception {
        Simple object = new Simple();
        object.number = 100;
        object.text = "Hello, world!";

        context.put("exporter", "exporter");
        BulkLoadExporterRetriever exporter = new BulkLoadExporterRetriever();
        try (ModelOutput<Simple> output = exporter.createOutput(SIMPLE, UNION_DESC)) {
            output.write(object);
        }
        assertThat(h2.count("SIMPLE"), is(1));
        assertThat(h2.count("DUP_CHECK"), is(0));

        List<Simple> list = retrieve(SIMPLE, "SIMPLE");
        assertThat(list, is(Arrays.asList(object)));
    }

    /**
     * output to dupcheck w/ union.
     * @throws Exception if occur
     */
    @Test
    public void output_union_dupcheck() throws Exception {
        DupCheck object = new DupCheck();
        object.number = 100;
        object.text = "Hello, world!";

        context.put("exporter", "exporter");
        BulkLoadExporterRetriever exporter = new BulkLoadExporterRetriever();
        try (ModelOutput<DupCheck> output = exporter.createOutput(DUP_CHECK, UNION_DESC)) {
            output.write(object);
        }
        assertThat(h2.count("SIMPLE"), is(0));
        assertThat(h2.count("DUP_CHECK"), is(1));

        List<DupCheck> list = retrieve(DUP_CHECK, "DUP_CHECK");
        assertThat(list, is(Arrays.asList(object)));
    }

    /**
     * attempted to output to unknown table w/ union.
     * @throws Exception if occur
     */
    @Test(expected = IOException.class)
    public void output_union_missing() throws Exception {
        context.put("exporter", "exporter");
        BulkLoadExporterRetriever exporter = new BulkLoadExporterRetriever();
        try (ModelOutput<?> output = exporter.createOutput(SIMPLE, MISSING_DESC)) {
            // do nothing
        }
    }

    /**
     * retrieve w/ union.
     * @throws Exception if occur
     */
    @Test
    public void source_union() throws Exception {
        Union object = new Union();
        object.number = 100;
        object.text = "Hello, world!";
        insert(object, UNION, "SIMPLE");

        context.put("exporter", "exporter");
        BulkLoadExporterRetriever exporter = new BulkLoadExporterRetriever();
        try (DataModelSource source = exporter.createSource(UNION, UNION_DESC)) {
            List<Union> results = drain(UNION, source);
            assertThat(results, is(Arrays.asList(object)));
        }
    }

    /**
     * retrieve from normal w/ union.
     * @throws Exception if occur
     */
    @Test
    public void source_union_normal() throws Exception {
        Simple object = new Simple();
        object.number = 100;
        object.text = "Hello, world!";
        insert(object, SIMPLE, "SIMPLE");

        context.put("exporter", "exporter");
        BulkLoadExporterRetriever exporter = new BulkLoadExporterRetriever();
        try (DataModelSource source = exporter.createSource(SIMPLE, UNION_DESC)) {
            List<Simple> results = drain(SIMPLE, source);
            assertThat(results, is(Arrays.asList(object)));
        }
    }

    /**
     * retrieve from dupcheck w/ union.
     * @throws Exception if occur
     */
    @Test
    public void source_union_dupcheck() throws Exception {
        DupCheck object = new DupCheck();
        object.number = 100;
        object.text = "Hello, world!";
        insert(object, DUP_CHECK, "DUP_CHECK");

        context.put("exporter", "exporter");
        BulkLoadExporterRetriever exporter = new BulkLoadExporterRetriever();
        try (DataModelSource source = exporter.createSource(DUP_CHECK, UNION_DESC)) {
            List<DupCheck> results = drain(DUP_CHECK, source);
            assertThat(results, is(Arrays.asList(object)));
        }
    }

    /**
     * attempt to retrieve from missing table w/ union.
     * @throws Exception if occur
     */
    @Test(expected = IOException.class)
    public void source_union_missing() throws Exception {
        context.put("exporter", "exporter");
        BulkLoadExporterRetriever exporter = new BulkLoadExporterRetriever();
        try (DataModelSource source = exporter.createSource(SIMPLE, MISSING_DESC)) {
            // do nothing
        }
    }

    private <T> void insert(T simple, DataModelDefinition<T> def, String table) {
        TableInfo<T> info = new TableInfo<>(def, table, Arrays.asList(
                "NUMBER",
                "TEXT",
                "C_BOOL",
                "C_BYTE",
                "C_SHORT",
                "C_LONG",
                "C_FLOAT",
                "C_DOUBLE",
                "C_DECIMAL",
                "C_DATE",
                "C_TIME",
                "C_DATETIME"));
        try (TableOutput<T> output = new TableOutput<>(info, h2.open());) {
            output.write(simple);
        } catch (Exception e) {
            throw new AssertionError(e);
        }
    }

    private <T> List<T> retrieve(DataModelDefinition<T> def, String table) {
        TableInfo<T> info = new TableInfo<>(def, table, Arrays.asList(
                "NUMBER",
                "TEXT",
                "C_BOOL",
                "C_BYTE",
                "C_SHORT",
                "C_LONG",
                "C_FLOAT",
                "C_DOUBLE",
                "C_DECIMAL",
                "C_DATE",
                "C_TIME",
                "C_DATETIME"));
        try (TableSource<T> source = new TableSource<>(info, h2.open())) {
            return drain(def, source);
        } catch (Exception e) {
            throw new AssertionError(e);
        }
    }

    private <T> List<T> drain(DataModelDefinition<T> def, DataModelSource source) throws IOException {
        try {
            List<DataModelReflection> retrieved = new ArrayList<>();
            while (true) {
                DataModelReflection next = source.next();
                if (next == null) {
                    break;
                }
                retrieved.add(next);
            }
            Collections.sort(retrieved, new Comparator<DataModelReflection>() {
                @Override
                public int compare(DataModelReflection o1, DataModelReflection o2) {
                    PropertyName name = PropertyName.newInstance("number");
                    Integer i1 = (Integer) o1.getValue(name);
                    Integer i2 = (Integer) o2.getValue(name);
                    return i1.compareTo(i2);
                }
            });
            List<T> results = new ArrayList<>();
            for (DataModelReflection r : retrieved) {
                results.add(def.toObject(r));
            }
            return results;
        } finally {
            source.close();
        }
    }
}
