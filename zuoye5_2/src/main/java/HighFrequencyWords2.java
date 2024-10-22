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

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashSet;
import java.util.Map;
import java.util.TreeMap;

import java.util.regex.Matcher;
import java.util.regex.Pattern;
// ./hadoop jar /home/njucs/zuoye5_2/target/zuoye5_2-1.0-SNAPSHOT.jar HighFrequencyWords2 /input /output2 /user/root/stop-word-list.txt
public class HighFrequencyWords2 {

    public static class TokenizerMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();
        private HashSet<String> stopWords = new HashSet<>();

        @Override
        protected void setup(Context context) throws IOException {
            // Load stop words from the stop-word-list.txt file
            Path[] stopWordPaths = context.getLocalCacheFiles();
            if (stopWordPaths != null && stopWordPaths.length > 0) {
                try (BufferedReader br = new BufferedReader(new FileReader(stopWordPaths[0].toString()))) {
                    String line;
                    while ((line = br.readLine()) != null) {
                        stopWords.add(line.trim().toLowerCase());
                    }
                }
            }
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String regex =  ",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)";
            // String[] columns = line.split(","); // Assuming CSV format with ',' as delimiter
            String[] columns = line.split(regex);
            if (columns.length >= 2) { // Ensure there are enough columns
                String headline = columns[1].trim().toLowerCase(); // Title is in the 2nd column (index 1)
                // Remove punctuation and split into words
                String[] words = headline.replaceAll("[^a-zA-Z0-9 ]", "").split("\\s+");
                for (String w : words) {
                    if (!stopWords.contains(w) && !w.isEmpty()) {
                        word.set(w);
                        context.write(word, one);
                    }
                }
            }
        }
    }

    public static class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private TreeMap<Integer, String> countMap = new TreeMap<>(); // Sorted by count

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            countMap.put(sum, key.toString());
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            // Output the top 100 words
            int rank = 1;
            for (Map.Entry<Integer, String> entry : countMap.descendingMap().entrySet()) {
                if (rank > 100) break;
                context.write(new Text(rank + ": " + entry.getValue() + ", " + entry.getKey()), null);
                rank++;
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "high frequency words");

        job.setJarByClass(HighFrequencyWords2.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setReducerClass(WordCountReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // Set stop word file as a cache file
        job.addCacheFile(new Path(args[2]).toUri());

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}