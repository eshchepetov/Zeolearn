package org.eugene.zeolearn.mrtask;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Main application
 *
 */

public class AverageTempApplication  extends Configured implements Tool
{
    public static void main(String[] args) throws Exception {
        int result = ToolRunner.run(new AverageTempApplication(), args);
        System.out.print("Result:" + result);
    }

    @SuppressWarnings("deprecation")
    public int run(String[] args) throws Exception {
        Configuration conf = new Configuration();

        Job job = new Job(conf, "Average temperature for a year");

        job.setMapperClass(AverageTempMapper.class);
        job.setReducerClass(AverageTempReducer.class);

        job.setInputFormatClass(TextInputFormat.class);
//        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // job.setNumReduceTasks(2);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        job.setJarByClass(AverageTempApplication.class);

        boolean result = job.waitForCompletion(true);
        return result ? 0 : 1;
    }
}
