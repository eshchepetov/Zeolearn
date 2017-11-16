package org.eugene.zeolearn.mrtask;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.stream.StreamSupport;

import org.apache.hadoop.io.IntWritable;

/**
 * Reducer class
 */
public class AverageTempReducer extends Reducer<Text, IntWritable, Text, DoubleWritable> {
    public void reduce(Text key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {

//        Double avg = StreamSupport.stream(values.spliterator(), false)
//                .mapToDouble(a -> a)
//                .average();

        int count = 0;
        Double sumTemperature = 0D;

        for (IntWritable x : values) {
            sumTemperature += x.get();
            count++;
        }

        context.write(key, new DoubleWritable(sumTemperature / count));
    }
}