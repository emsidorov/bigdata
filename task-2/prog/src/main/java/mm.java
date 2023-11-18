import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.TaskCounter;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormatCounter;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.BufferedWriter;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.HashMap;
import java.util.Scanner;


class ElementKey implements WritableComparable<ElementKey> {
    public final IntWritable group_l = new IntWritable();
    public final IntWritable group_r = new IntWritable();

    public ElementKey() {}

    public ElementKey(int _group_l, int _group_r) {
        group_l.set(_group_l);
        group_r.set(_group_r);
    }

    @Override
    public int compareTo(ElementKey other) {
        int result = this.group_l.compareTo(other.group_l); 
        if (result == 0) result = this.group_r.compareTo(other.group_r);
        return result;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        group_l.write(dataOutput);
        group_r.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        group_l.readFields(dataInput);
        group_r.readFields(dataInput);
    }

}


class ElementValue implements Writable {
    public final BooleanWritable matrix = new BooleanWritable();
    public final IntWritable inner_idx = new IntWritable();
    public final IntWritable outer_idx = new IntWritable();
    public final DoubleWritable value = new DoubleWritable();

    public ElementValue() {}

    public ElementValue(boolean _matrix, int _inner_idx, int _outer_idx, double _value) {
        matrix.set(_matrix);
        inner_idx.set(_inner_idx);
        outer_idx.set(_outer_idx);
        value.set(_value);
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        matrix.write(dataOutput);
        inner_idx.write(dataOutput);
        outer_idx.write(dataOutput);
        value.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        matrix.readFields(dataInput);
        inner_idx.readFields(dataInput);
        outer_idx.readFields(dataInput);
        value.readFields(dataInput);
    }
}

public class mm {
    private static final Path data_path = new Path(Path.SEPARATOR + "data");
    private static final Path size_path = new Path(Path.SEPARATOR + "size");

    private static int groups;
    private static String tags;
    private static String float_format;
    private static int num_reducers;
    private static boolean verbose;
    private static boolean remove_output;
    private static int size_l;
    private static int size_r;
    private static int size_inner;
    private static int line_size_l;
    private static int line_size_r;
    

    public static void __init_config(Configuration conf) {
        groups = conf.getInt("mm.groups", 1);
        tags = conf.get("mm.tags", "ABC");
        float_format = conf.get("mm.float-format", "%.3f");
        num_reducers = conf.getInt("mapred.reduce.tasks", 1);
        verbose = conf.getBoolean("mm.verbose", true);
        remove_output = conf.getBoolean("mm.remove.output", true);
        size_l = conf.getInt("mm.size.left", 1);
        size_r = conf.getInt("mm.size.right", 1);
        size_inner = conf.getInt("mm.size.inner", 1);

        line_size_l = (int) Math.ceil((double)size_l / groups);
        line_size_r = (int) Math.ceil((double)size_r / groups);
    }

    public static int[] read_size(FileSystem fs, Path path) {
        Scanner size_scan = null;
        try (DataInputStream stream = fs.open(Path.mergePaths(path, size_path))) {
            size_scan = new Scanner(stream);
            int height = size_scan.nextInt();
            int width = size_scan.nextInt();
            return new int[]{height, width};
        } catch (IOException err) {
            throw new IllegalStateException("Couldn't read the size for " + path, err);
        } finally {
            if (size_scan != null) {
                size_scan.close();
            }
        }
    }

    public static class MatrixMapper extends Mapper<Object, Text, ElementKey, ElementValue> {

        @Override
        protected void setup(
            Mapper<Object, Text, ElementKey, ElementValue>.Context context
        ) throws IOException, InterruptedException {
            super.setup(context);
            __init_config(context.getConfiguration());
        }

        @Override
        public void map(
            Object row_id, Text row, Context context
        ) throws IOException, InterruptedException {
            String[] row_arr = row.toString().split("\t");
            
            boolean matrix;
            char tag = row_arr[0].charAt(0);

            if (tag == tags.charAt(0)) {
                matrix = true;
            } else if (tag == tags.charAt(1)) {
                matrix = false;
            } else {
                return;
            }

            int row_idx = Integer.parseInt(row_arr[1]), col_idx = Integer.parseInt(row_arr[2]);
            int group_l, group_r;
            double value = Float.parseFloat(row_arr[3]);

            if (matrix) {
                group_l = row_idx / line_size_l;
                ElementValue val = new ElementValue(
                    matrix, col_idx, row_idx, value
                );
                for (group_r = 0; group_r < groups; ++group_r) {
                    ElementKey key = new ElementKey(group_l, group_r);
                    context.write(key, val);
                }
            } else {
                group_r = col_idx / line_size_r;
                ElementValue val = new ElementValue(
                    matrix, row_idx, col_idx, value
                );
                for (group_l = 0; group_l < groups; ++group_l) {
                    ElementKey key = new ElementKey(group_l, group_r);
                    context.write(key, val);
                }
            }
        }
    }
    
    public static class MatrixPartitioner extends Partitioner<ElementKey, ElementValue> {
        @Override
        public int getPartition(ElementKey key, ElementValue value, int num_partitions) {
            return (key.group_l.get() * groups + key.group_r.get()) % num_partitions;
        }
    }

    public static class MatrixReducer extends Reducer<ElementKey, ElementValue, Text, Text> {
        @Override
        protected void setup(
            Reducer<ElementKey, ElementValue, Text, Text>.Context context
        ) throws IOException, InterruptedException {
            super.setup(context);
            __init_config(context.getConfiguration());
        }

        @Override
        protected void reduce(
            ElementKey key, 
            Iterable<ElementValue> values, 
            Reducer<ElementKey, ElementValue, Text, Text>.Context context
        ) throws IOException, InterruptedException {
            int group_l = key.group_l.get(), group_r = key.group_r.get();

            int line_l = Math.min((group_l + 1) * line_size_l, size_l) - group_l * line_size_l;
            int line_r = Math.min((group_r + 1) * line_size_r, size_r) - group_r * line_size_r;

            double[] left_line = new double[line_l * size_inner];
            double[] right_line = new double[line_r * size_inner];

            for (ElementValue val : values) {
                if (val.matrix.get()) {
                    left_line[
                        (val.outer_idx.get() % line_size_l) * size_inner + val.inner_idx.get()
                    ] = val.value.get();
                } else {
                    right_line[
                        (val.outer_idx.get() % line_size_l) * size_inner + val.inner_idx.get()
                    ] = val.value.get();
                }
            }

            Text output_key = new Text("" + tags.charAt(2));
            Text output_val = new Text();
            int line_shift_l = line_size_l * group_l;
            int line_shift_r = line_size_r * group_r;

            for (int l = 0; l < line_l; ++l) {
                for (int r = 0; r < line_r; ++r) {
                    int l_shift = l * size_inner;
                    int r_shift = r * size_inner;
                    double result = 0;

                    for (int i = 0; i < size_inner; ++i) {
                        result += left_line[l_shift + i] * right_line[r_shift + i];
                    }


                    if (result != 0) {
                        output_val.set(
                            "" + (line_shift_l + l) + '\t' + (line_shift_r + r) + '\t' + String.format(float_format, result)
                        );
                        context.write(output_key, output_val);
                    }
                }
            }
        }
    }

    public static void main(String[] args) throws Exception {
        long start_time = System.nanoTime();

        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        conf.set("mapreduce.output.textoutputformat.separator", "\t");
        Path input_left = new Path(otherArgs[0]);
        Path input_right = new Path(otherArgs[1]);
        Path output_path = new Path(otherArgs[2]);

        FileSystem fs = FileSystem.get(conf);
        int[] size_left = read_size(fs, input_left);
        int[] size_right = read_size(fs, input_right);

        conf.setInt("mm.size.left", size_left[0]);
        conf.setInt("mm.size.right", size_right[1]);
        conf.setInt("mm.size.inner", size_right[0]);
        __init_config(conf);

        if (fs.exists(output_path) && remove_output)
            fs.delete(output_path, true);


        Job job = Job.getInstance(conf, "mm");
        job.setNumReduceTasks(num_reducers);
        job.setJarByClass(mm.class);
        TextInputFormat.addInputPath(job, Path.mergePaths(input_left, data_path));
        TextInputFormat.addInputPath(job, Path.mergePaths(input_right, data_path));
        job.setMapperClass(MatrixMapper.class);
        job.setMapOutputKeyClass(ElementKey.class);
        job.setMapOutputValueClass(ElementValue.class);
        job.setPartitionerClass(MatrixPartitioner.class);
        job.setReducerClass(MatrixReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileOutputFormat.setOutputPath(job, Path.mergePaths(output_path, data_path));

        BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(fs.create(Path.mergePaths(output_path, size_path))));
        writer.write(size_left[0] + "\t" + size_right[1]);
        writer.close();
        fs.close();
    
        int exit_code = job.waitForCompletion(verbose) ? 0 : 1;

        long end_time = System.nanoTime();
        System.out.println("time: " + (end_time - start_time) / 1_000_000_000.0  + " sec");

        Counters counters = job.getCounters();
        HashMap<String, Long> counterMap = new HashMap<>();

        TaskCounter[] taskCounters = {
            TaskCounter.MAP_INPUT_RECORDS,
            TaskCounter.MAP_OUTPUT_RECORDS,
            TaskCounter.MAP_OUTPUT_BYTES,
            TaskCounter.REDUCE_INPUT_GROUPS,
            TaskCounter.REDUCE_SHUFFLE_BYTES,
            TaskCounter.REDUCE_INPUT_RECORDS,
            TaskCounter.REDUCE_OUTPUT_RECORDS
        };

        for (TaskCounter taskCounter : taskCounters) {
            Counter counter = counters.findCounter(taskCounter);
            counterMap.put(taskCounter.name(), counter.getValue());
        }

        Counter bytesReadCounter = counters.findCounter(FileInputFormatCounter.BYTES_READ);
        counterMap.put(FileInputFormatCounter.BYTES_READ.name(), bytesReadCounter.getValue());

        StringBuilder results = new StringBuilder();
        for (HashMap.Entry<String, Long> entry : counterMap.entrySet()) {
            results.append("" + entry.getKey() + ": " + entry.getValue() + "\n");
        }
        System.out.println(results);

        System.exit(exit_code);
    }
}
