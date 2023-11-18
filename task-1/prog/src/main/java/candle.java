import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Iterator;


class DealKey implements WritableComparable<DealKey> {
    public final Text symbol;
    public final LongWritable candle_id;
    public final LongWritable moment;
    public final LongWritable id_deal;

    public DealKey() {
        this.symbol = new Text();
        this.candle_id = new LongWritable();
        this.moment = new LongWritable();
        this.id_deal = new LongWritable();
    }

    public DealKey(String _symbol, long _candle_id, long _moment, long _id_deal) {
        this.symbol = new Text(_symbol);
        this.candle_id = new LongWritable(_candle_id);
        this.moment = new LongWritable(_moment);
        this.id_deal = new LongWritable(_id_deal);
    }

    @Override
    public int compareTo(DealKey other) {
        int result = this.symbol.compareTo(other.symbol);
        if (result == 0) result = this.candle_id.compareTo(other.candle_id);
        if (result == 0) result = this.moment.compareTo(other.moment);
        if (result == 0) result = this.id_deal.compareTo(other.id_deal);
        return result;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        symbol.write(dataOutput);
        candle_id.write(dataOutput);
        moment.write(dataOutput);
        id_deal.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        symbol.readFields(dataInput);
        candle_id.readFields(dataInput);
        moment.readFields(dataInput);
        id_deal.readFields(dataInput);
    }
}


class DealValue extends DoubleWritable {
    DealValue() {}

    DealValue(double price_deal) {
        super(price_deal);
    }
}

public class candle {
    private static final SimpleDateFormat datetime_fmt = new SimpleDateFormat("yyyyMMddkkmmssSSS");
    private static final SimpleDateFormat date_fmt = new SimpleDateFormat("yyyyMMdd");
    private static final SimpleDateFormat time_fmt = new SimpleDateFormat("kkmm");
    private static final SimpleDateFormat time_sec_fmt = new SimpleDateFormat("kkmmssSSS");

    private static long candle_width;
    private static String securities;
    private static long date_from;
    private static long date_to;
    private static long time_from;
    private static long time_to;
    private static int num_reducers;
    private static boolean verbose;
    private static boolean remove_output;
    

    public static void __init_config(Configuration conf) {
        candle_width = conf.getLong("candle.width", 300000);
        securities = conf.get("candle.securities", ".*");
        date_from = getDateTimeSeconds(conf.get("candle.date.from", "19000101"), date_fmt);
        date_to = getDateTimeSeconds(conf.get("candle.date.to", "20200101"), date_fmt);
        time_from = getDateTimeSeconds(conf.get("candle.time.from", "1000"), time_fmt);
        time_to = getDateTimeSeconds(conf.get("candle.time.to", "1800"), time_fmt);
        num_reducers = conf.getInt("candle.num.reducers", 1);
        verbose = conf.getBoolean("candle.verbose", true);
        remove_output = conf.getBoolean("candle.remove.output", true);
    }
    
    public static long getDateTimeSeconds(String date, SimpleDateFormat fmt) {
        try {
            return fmt.parse(date).getTime();
        } catch (ParseException err) {
            throw new IllegalStateException("Date/Time/DateTime/Seconds have strange format:-(", err);
        }
    }

    public static class CandleMapper extends Mapper<Object, Text, DealKey, DealValue> {

        @Override
        protected void setup(
            Mapper<Object, Text, DealKey, DealValue>.Context context
        ) throws IOException, InterruptedException {
            super.setup(context);
            __init_config(context.getConfiguration());
        }

        @Override
        public void map(
            Object row_id, Text row, Context context
        ) throws IOException, InterruptedException {
            String row_str = row.toString();

            if (row_str.startsWith("#SYMBOL")) return;

            String[] row_arr = row_str.split(",");

            String symbol = row_arr[0], moment_str = row_arr[2];
            long id_deal = Long.parseLong(row_arr[3]);
            double price_deal = Double.parseDouble(row_arr[4]);

            long moment_date = getDateTimeSeconds(moment_str.substring(0, 8), date_fmt);
            long moment_time = getDateTimeSeconds(moment_str.substring(8, 17), time_sec_fmt);
            long moment = getDateTimeSeconds(moment_str, datetime_fmt);

            if (!(
                symbol.matches(securities) &&
                moment_date >= date_from && moment_date < date_to &&
                moment_time >= time_from && moment_time < time_to
            )) return;

            long candle_id = (moment_date + moment_time) / candle_width;
            
            context.write(
                new DealKey(symbol, candle_id, moment, id_deal), new DealValue(price_deal)
            );
        }
    }

    public static class CandleGroupingComparator extends WritableComparator {
        public CandleGroupingComparator() {
            super(DealKey.class, true);
        }

        @Override
        public int compare(WritableComparable _left, WritableComparable _right) {
            DealKey left = (DealKey) _left;
            DealKey right = (DealKey) _right;
            int result = left.candle_id.compareTo(right.candle_id);
            if (result == 0) result = left.symbol.compareTo(right.symbol);
            return result;
        }
    }
    
    public static class CandlePartitioner extends Partitioner<DealKey, DealValue> {
        @Override
        public int getPartition(DealKey key, DealValue value, int num_partitions) {
            return key.candle_id.hashCode() % num_partitions;
        }
    }

    public static class CandleReducer extends Reducer<DealKey, DealValue, Text, Text> {
        public MultipleOutputs<Text, Text> outputs;

        @Override
        protected void setup(
            Reducer<DealKey, DealValue, Text, Text>.Context context
        ) throws IOException, InterruptedException {
            super.setup(context);
            __init_config(context.getConfiguration());
            outputs = new MultipleOutputs<>(context);
        }

        @Override
        protected void cleanup(
            Reducer<DealKey, DealValue, Text, Text>.Context context
        ) throws IOException, InterruptedException {
            outputs.close();
            super.cleanup(context);
        }

        @Override
        protected void reduce(
            DealKey key, 
            Iterable<DealValue> values, 
            Reducer<DealKey, DealValue, Text, Text>.Context context
        ) throws IOException, InterruptedException {
            Iterator<DealValue> it = values.iterator();

            double open = it.next().get();
            double cur_val = open, low = open, high = open;

            while (it.hasNext()) {
                cur_val = it.next().get();
                low = Math.min(low, cur_val);
                high = Math.max(high, cur_val);
            }

            double close = cur_val;
            double candle = key.candle_id.get();

            String symbol    = key.symbol.toString();
            String datetime = datetime_fmt.format(new Date(
                (long) (candle * candle_width)
            ));
            String open_str  = String.format("%.1f", open);
            String close_str = String.format("%.1f", close);
            String low_str   = String.format("%.1f", low);
            String high_str  = String.format("%.1f", high);
    
            outputs.write(
                new Text(symbol + "," + datetime),
                new Text(open_str + "," + high_str + "," + low_str + "," + close_str),
                symbol
            );
        }
    }

    public static void main(String[] args) throws Exception {
        long start_time = System.nanoTime();

        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        conf.set("mapreduce.output.textoutputformat.separator", ",");
        Path input_path = new Path(otherArgs[0]);
        Path output_path = new Path(otherArgs[1]);
        __init_config(conf);

        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(output_path) && remove_output)
            fs.delete(output_path, true);

        Job job = Job.getInstance(conf, "candle");
        job.setNumReduceTasks(num_reducers);
        job.setJarByClass(candle.class);
        TextInputFormat.addInputPath(job, input_path);
        job.setMapperClass(CandleMapper.class);
        job.setMapOutputKeyClass(DealKey.class);
        job.setMapOutputValueClass(DealValue.class);
        job.setGroupingComparatorClass(CandleGroupingComparator.class);
        job.setPartitionerClass(CandlePartitioner.class);
        job.setReducerClass(CandleReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileOutputFormat.setOutputPath(job, output_path);
    
        int exit_code = job.waitForCompletion(verbose) ? 0 : 1;

        long end_time = System.nanoTime();
        System.out.println("time: " + (end_time - start_time) / 1_000_000_000.0  + " sec");

        System.exit(exit_code);
    }
}
