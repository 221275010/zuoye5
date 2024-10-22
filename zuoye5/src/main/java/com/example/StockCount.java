package com.example;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.*;

// ./hadoop jar /home/njucs/zuoye5/target/zuoye5-1.0-SNAPSHOT.jar com.example.StockCount  /input /output

public class StockCount {

    public static class StockMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private Text stockCode = new Text();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] columns = line.split(","); // Assuming CSV format with ',' as delimiter
            if (columns.length >= 4) { // Ensure there are enough columns
                stockCode.set(columns[columns.length - 1].trim()); // Assuming stock code is in the last column (
                context.write(stockCode, one);
            }
        }
    }

    public static class StockReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();
        private Map<String, Integer> stockCountMap = new HashMap<>();

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            stockCountMap.put(key.toString(), sum); // Store the stock code and its count
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            // Sort the stock count map by value (count)
            List<Map.Entry<String, Integer>> sortedList = new ArrayList<>(stockCountMap.entrySet());
            sortedList.sort((a, b) -> b.getValue().compareTo(a.getValue())); // Sort by count descending

            // Output the results in the required format
            int rank = 1;
            for (Map.Entry<String, Integer> entry : sortedList) {
                context.write(new Text(rank + ": " + entry.getKey() + ", " + entry.getValue()), null);
                rank++;
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "stock count");

        job.setJarByClass(StockCount.class);
        job.setMapperClass(StockMapper.class);
        job.setReducerClass(StockReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}