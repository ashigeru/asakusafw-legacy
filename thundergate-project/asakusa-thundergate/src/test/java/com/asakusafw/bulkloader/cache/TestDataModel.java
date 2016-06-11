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
package com.asakusafw.bulkloader.cache;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VLongWritable;
import org.apache.hadoop.io.Writable;

import com.asakusafw.runtime.value.DateTime;
import com.asakusafw.runtime.value.DateTimeOption;
import com.asakusafw.thundergate.runtime.cache.ThunderGateCacheSupport;

/**
 * Cache testing data model.
 */
@SuppressWarnings("deprecation")
public class TestDataModel implements Writable, ThunderGateCacheSupport, Comparable<TestDataModel> {

    /**
     * system ID.
     */
    public final VLongWritable systemId = new VLongWritable();

    /**
     * content.
     */
    public final Text value = new Text();

    /**
     * modified.
     */
    public final DateTimeOption timestamp = new DateTimeOption(new DateTime());

    /**
     * deleted.
     */
    public final BooleanWritable deleted = new BooleanWritable();

    /**
     * Set SID and value.
     * @param text the value
     * @return this
     */
    public TestDataModel next(String text) {
        this.systemId.set(this.systemId.get() + 1);
        this.value.set(text);
        return this;
    }

    /**
     * Set timestamp.
     * @param year year (1-...)
     * @param month month (1-12)
     * @param day day (1-31)
     * @return this
     */
    public TestDataModel on(int year, int month, int day) {
        timestamp.modify(new DateTime(year, month, day, 0, 0, 0));
        return this;
    }

    /**
     * Creates a copy of this.
     * @return the copy
     */
    public TestDataModel copy() {
        TestDataModel copy = new TestDataModel();
        copy.systemId.set(systemId.get());
        copy.value.set(value);
        copy.timestamp.copyFrom(timestamp);
        copy.deleted.set(deleted.get());
        return copy;
    }

    @Override
    public long __tgc__DataModelVersion() {
        return 1L;
    }

    @Override
    public String __tgc__TimestampColumn() {
        return "DUMMY";
    }

    @Override
    public long __tgc__SystemId() {
        return systemId.get();
    }

    @Override
    public long __tgc__Timestamp() {
        return timestamp.get().getElapsedSeconds();
    }

    @Override
    public boolean __tgc__Deleted() {
        return deleted.get();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        systemId.write(out);
        value.write(out);
        timestamp.write(out);
        deleted.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        systemId.readFields(in);
        value.readFields(in);
        timestamp.readFields(in);
        deleted.readFields(in);
    }

    @Override
    public int compareTo(TestDataModel o) {
        return systemId.compareTo(o.systemId);
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("TestDataModel [systemId=");
        builder.append(systemId);
        builder.append(", value=");
        builder.append(value);
        builder.append(", timestamp=");
        builder.append(timestamp);
        builder.append(", deleted=");
        builder.append(deleted);
        builder.append("]");
        return builder.toString();
    }
}
