package crunch;

import org.apache.crunch.*;
import org.apache.crunch.impl.mr.MRPipeline;
import org.apache.crunch.io.To;
import org.apache.crunch.test.TemporaryPath;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

import java.io.Serializable;

import static org.apache.crunch.types.writable.Writables.*;

public class CheckpointTest implements Serializable {

    @Rule
    public transient TemporaryPath tmpDir = new TemporaryPath();

    @Rule
    public transient TestName name = new TestName();

    @Test
    public void testCheckpoint() throws Exception {

        String inputPath = tmpDir.copyResourceFileName("set1.txt");
        Thread.sleep(2000);

        PipelineResult result1 = runCheckpointedPipeline(
                new MRPipeline(getClass()), inputPath);
        assertEquals(2, result1.getStageResults().size());

        PipelineResult result2 = runCheckpointedPipeline(
                new MRPipeline(getClass()), inputPath);
        assertEquals(1, result2.getStageResults().size());

    }

    private PipelineResult runCheckpointedPipeline(Pipeline pipeline,
                                                   String inputPath) throws Exception {
        String checkpointPath = tmpDir.getFileName("checkpoint");
        String outputPath = tmpDir.getFileName("hist");
        PCollection<String> lines = pipeline.readTextFile(inputPath);
        PTable<String, Long> counts = lines.count();
        PTable<Long, String> inverseCounts = counts.parallelDo(
                new InversePairFn<String, Long>(), tableOf(longs(), strings()));
        inverseCounts.write(To.sequenceFile(checkpointPath), Target.WriteMode.CHECKPOINT);
        PTable<Long, Integer> hist = inverseCounts
                .groupByKey()
                .mapValues(new CountValuesFn<String>(), ints());
        hist.write(To.textFile(outputPath), Target.WriteMode.OVERWRITE);
        return pipeline.done();
    }

}
