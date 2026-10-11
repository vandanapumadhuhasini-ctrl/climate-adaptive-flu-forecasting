package climaflu.mr;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * MapReduce job 1: per (element, year) count and sum of kept US observations over the raw GHCN-Daily CSVs.
 * Output lines: element|year \t count \t sum. Must reproduce the Spark / pandas benchmark checksum.
 * Usage: hadoop jar climaflu-mr.jar climaflu.mr.ElementYearAggregate <input> <output> [reducers]
 */
public class ElementYearAggregate extends Configured implements Tool {

    public static class Map extends Mapper<LongWritable, Text, Text, CountSumWritable> {
        private final GhcnRecord rec = new GhcnRecord();
        private final Text key = new Text();
        private final CountSumWritable one = new CountSumWritable();

        @Override
        protected void map(LongWritable offset, Text line, Context ctx) throws IOException, InterruptedException {
            GhcnRecord.Outcome o = rec.parse(line.toString());
            ctx.getCounter("ClimaFlu", o.name()).increment(1);
            if (o != GhcnRecord.Outcome.KEPT) return;
            key.set(rec.element + "|" + rec.year);
            one.set(1, rec.value);
            ctx.write(key, one);
        }
    }

    /** Used as both combiner and reducer: (count, sum) pairs add up. */
    public static class Sum extends Reducer<Text, CountSumWritable, Text, CountSumWritable> {
        private final CountSumWritable out = new CountSumWritable();

        @Override
        protected void reduce(Text key, Iterable<CountSumWritable> vals, Context ctx) throws IOException, InterruptedException {
            long n = 0, s = 0;
            for (CountSumWritable v : vals) { n += v.getCount(); s += v.getSum(); }
            out.set(n, s);
            ctx.write(key, out);
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = getConf();
        Path in = new Path(args[0]), out = new Path(args[1]);
        FileSystem.get(conf).delete(out, true);
        Job job = Job.getInstance(conf, "ClimaFlu MR1: element x year aggregate (GHCN raw)");
        job.setJarByClass(ElementYearAggregate.class);
        job.setMapperClass(Map.class);
        job.setCombinerClass(Sum.class);
        job.setReducerClass(Sum.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(CountSumWritable.class);
        job.setNumReduceTasks(args.length > 2 ? Integer.parseInt(args[2]) : 2);
        FileInputFormat.addInputPath(job, in);
        FileOutputFormat.setOutputPath(job, out);
        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new Configuration(), new ElementYearAggregate(), args));
    }
}
