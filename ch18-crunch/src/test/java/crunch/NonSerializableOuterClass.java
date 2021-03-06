package crunch;

import com.google.common.collect.Lists;
import org.apache.crunch.*;
import org.apache.crunch.impl.mr.MRPipeline;
import org.apache.crunch.test.TemporaryPath;
import org.junit.Rule;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

import static org.apache.crunch.types.writable.Writables.strings;

public class NonSerializableOuterClass {

    @Rule
    public transient TemporaryPath tmpDir = new TemporaryPath();

    @Test(expected = CrunchRuntimeException.class)
    public void runPipeline() throws IOException {
        List<String> expectedContent = Lists.newArrayList("b", "c", "a", "e");
        String inputPath = tmpDir.copyResourceFileName("set1.txt");
        Pipeline pipeline = new MRPipeline(getClass());
        PCollection<String> lines = pipeline.readTextFile(inputPath);
        PCollection<String> lower = lines.parallelDo(new DoFn<String, String>() {
            @Override
            public void process(String input, Emitter<String> emitter) {
                emitter.emit(input.toLowerCase());
            }
        }, strings());
        assertEquals(expectedContent, Lists.newArrayList(lower.materialize()));
        pipeline.done();
    }

}
