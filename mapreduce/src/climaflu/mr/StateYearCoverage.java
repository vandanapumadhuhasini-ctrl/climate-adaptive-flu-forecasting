package climaflu.mr;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * MapReduce job 2: map-side (replicated) join of the raw observations with the fixed-width GHCN stations file,
 * shipped to every mapper through the distributed cache. Output: state|year|element \t count \t sum.
 * Station rule: id starts with "US" and the STATE field (columns 39-40) is non-blank.
 * Usage: hadoop jar climaflu-mr.jar climaflu.mr.StateYearCoverage <input> <stations.txt> <output> [reducers]
 */
public class StateYearCoverage extends Configured implements Tool {

    public static class Map extends Mapper<LongWritable, Text, Text, CountSumWritable> {
        private final HashMap<String, String> stationState = new HashMap<>();
        private final GhcnRecord rec = new GhcnRecord();
        private final Text key = new Text();
        private final CountSumWritable one = new CountSumWritable();

        @Override
        protected void setup(Context ctx) throws IOException {
            // "#stations" symlink name is created in the task working directory by the distributed cache.
            FileSystem local = FileSystem.getLocal(ctx.getConfiguration());
            try (BufferedReader r = new BufferedReader(new InputStreamReader(local.open(new Path("stations")), StandardCharsets.UTF_8))) {
                String l;
                while ((l = r.readLine()) != null) {
                    if (l.length() < 40 || !l.startsWith("US")) continue;
                    String state = l.substring(38, 40).trim();
                    if (!state.isEmpty()) stationState.put(l.substring(0, 11).trim(), state);
                }
            }
            ctx.getCounter("ClimaFlu", "STATIONS_LOADED_PER_MAPPER").setValue(stationState.size());
        }

        @Override
        protected void map(LongWritable offset, Text line, Context ctx) throws IOException, InterruptedException {
            if (rec.parse(line.toString()) != GhcnRecord.Outcome.KEPT) return;
            String state = stationState.get(rec.id);
            if (state == null) { ctx.getCounter("ClimaFlu", "NO_STATE").increment(1); return; }
            ctx.getCounter("ClimaFlu", "JOINED").increment(1);
            key.set(state + "|" + rec.year + "|" + rec.element);
            one.set(1, rec.value);
            ctx.write(key, one);
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = getConf();
        Path in = new Path(args[0]), out = new Path(args[2]);
        FileSystem.get(conf).delete(out, true);
        Job job = Job.getInstance(conf, "ClimaFlu MR2: state x year x element coverage (map-side join)");
        job.setJarByClass(StateYearCoverage.class);
        job.addCacheFile(new URI(args[1] + "#stations"));
        job.setMapperClass(Map.class);
        job.setCombinerClass(ElementYearAggregate.Sum.class);
        job.setReducerClass(ElementYearAggregate.Sum.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(CountSumWritable.class);
        job.setNumReduceTasks(args.length > 3 ? Integer.parseInt(args[3]) : 2);
        FileInputFormat.addInputPath(job, in);
        FileOutputFormat.setOutputPath(job, out);
        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new Configuration(), new StateYearCoverage(), args));
    }
}
