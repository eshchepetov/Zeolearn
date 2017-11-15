package org.eugene.zeolearn.mrtask;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;

/**
 * Reducer class
 */
public class AverageTempReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    public void reduce(Text key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {
        int max = 0;
        for (IntWritable x : values) {
            if (x.get() > max)
                max = x.get();

        }
        context.write(key, new IntWritable(max));
    }
}