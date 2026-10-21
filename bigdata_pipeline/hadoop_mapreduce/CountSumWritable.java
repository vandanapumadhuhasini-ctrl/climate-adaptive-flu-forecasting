package climaflu.mr;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

/** (count, sum) pair so one shuffle record carries both aggregates; summable, so it works as a combiner value. */
public class CountSumWritable implements Writable {
    private long count;
    private long sum;

    public CountSumWritable() { }

    public CountSumWritable(long count, long sum) { this.count = count; this.sum = sum; }

    public void set(long count, long sum) { this.count = count; this.sum = sum; }

    public long getCount() { return count; }

    public long getSum() { return sum; }

    @Override
    public void write(DataOutput out) throws IOException { out.writeLong(count); out.writeLong(sum); }

    @Override
    public void readFields(DataInput in) throws IOException { count = in.readLong(); sum = in.readLong(); }

    @Override
    public String toString() { return count + "\t" + sum; }
}
