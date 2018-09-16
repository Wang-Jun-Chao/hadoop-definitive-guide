package crunch;

import org.apache.crunch.*;
import org.apache.crunch.fn.Aggregators;
import org.apache.crunch.impl.mr.MRPipeline;

import static org.apache.crunch.types.writable.Writables.*;

// Crunch version of ch02-mr-intro MaxTemperature
public class MaxTemperatureCrunchWithShutdownHook {

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: MaxTemperatureCrunchWithShutdownHook <input path> <output path>");
            System.exit(-1);
        }

        Pipeline pipeline = new MRPipeline(MaxTemperatureCrunchWithShutdownHook.class);
        PCollection<String> records = pipeline.readTextFile(args[0]);

        PTable<String, Integer> yearTemperatures = records
                .parallelDo(toYearTempPairsFn(), tableOf(strings(), ints()));
        PTable<String, Integer> maxTemps = yearTemperatures
                .groupByKey()
                .combineValues(Aggregators.MAX_INTS());

        pipeline.writeTextFile(maxTemps, args[1]);
        final PipelineExecution execution = pipeline.runAsync();
        Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    System.out.println("Stopping pipeline...");
                    execution.kill();
                    execution.waitUntilDone();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }));
        execution.waitUntilDone();
        System.exit(execution.getResult().succeeded() ? 0 : 1);
    }

    static DoFn<String, Pair<String, Integer>> toYearTempPairsFn() {
        return new DoFn<String, Pair<String, Integer>>() {
            NcdcRecordParser parser = new NcdcRecordParser();

            @Override
            public void process(String input, Emitter<Pair<String, Integer>> emitter) {
                parser.parse(input);
                if (parser.isValidTemperature()) {
                    emitter.emit(Pair.of(parser.getYear(), parser.getAirTemperature()));
                }
            }
        };
    }

}
