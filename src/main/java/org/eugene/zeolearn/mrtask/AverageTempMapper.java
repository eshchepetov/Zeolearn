package org.eugene.zeolearn.mrtask;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Mapper class
 */

public class AverageTempMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        Integer temperature;

        String line = value.toString();
        String year = line.substring(15, 19);

        temperature = Integer.parseInt(line.charAt(87) == '+' ?
                line.substring(88, 92) : line.substring(87, 92));

        context.write(new Text(year), new IntWritable(temperature));
    }
}