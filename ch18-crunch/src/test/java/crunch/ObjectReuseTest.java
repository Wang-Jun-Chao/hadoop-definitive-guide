package crunch;

import org.apache.crunch.*;
import org.apache.crunch.impl.mr.MRPipeline;
import org.apache.crunch.test.TemporaryPath;
import org.apache.crunch.types.PType;
import org.apache.crunch.types.PTypeFamily;
import org.apache.crunch.types.avro.Avros;
import org.junit.Rule;
import org.junit.Test;

import java.io.IOException;
import java.io.Serializable;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class ObjectReuseTest implements Serializable {

    @Rule
    public transient TemporaryPath tmpDir = new TemporaryPath();

    public static <K, V> PTable<K, Collection<V>> uniqueValues(PTable<K, V> table) {
        PTypeFamily tf = table.getTypeFamily();
        final PType<V> valueType = table.getValueType();
        return table.groupByKey().mapValues("unique",
                new MapFn<Iterable<V>, Collection<V>>() {
                    @Override
                    public void initialize() {
                        valueType.initialize(getConfiguration());
                    }

                    @Override
                    public Set<V> map(Iterable<V> values) {
                        Set<V> collected = new HashSet<V>();
                        for (V value : values) {
                            collected.add(valueType.getDetachedValue(value));
                        }
                        return collected;
                    }
                }, tf.collections(table.getValueType()));
    }

    @Test
    public void test() throws IOException {
        String inputPath = tmpDir.copyResourceFileName("set2.txt");

        Pipeline pipeline = new MRPipeline(getClass());
        PCollection<String> lines = pipeline.readTextFile(inputPath);
        PTable<String, StringWrapper> table = lines.parallelDo(new DoFn<String, Pair<String,
                StringWrapper>>() {
            @Override
            public void process(String input, Emitter<Pair<String, StringWrapper>> emitter) {
                emitter.emit(Pair.of(input, new StringWrapper(input)));
            }
        }, Avros.tableOf(Avros.strings(), Avros.records(StringWrapper.class)));
        PTable<String, Collection<StringWrapper>> uniques = uniqueValues(table);

        Map<String, Collection<StringWrapper>> materialisedUniques = uniques.materializeToMap();
        assertEquals(1, materialisedUniques.get("b").size());

        pipeline.run();
    }

    public static class StringWrapper {
        private String value;

        public StringWrapper() {
        }

        public StringWrapper(String value) {
            this.value = value;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            StringWrapper myRecord = (StringWrapper) o;
            if (value != null ? !value.equals(myRecord.value) : myRecord.value != null) {
                return false;
            }
            return true;
        }

        @Override
        public int hashCode() {
            return value != null ? value.hashCode() : 0;
        }

        @Override
        public String toString() {
            return value;
        }
    }
}
