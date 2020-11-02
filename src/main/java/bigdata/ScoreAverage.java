package bigdata;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.conf.Configuration;

public class ScoreAverage {
    public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
            String[] str = value.toString().split(" ");
            context.write(new Text(str[1]), new IntWritable(Integer.parseInt(str[2])));
        }
    }

    public static class IntSumReducer extends Reducer<Text, IntWritable, Text, FloatWritable> {

        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int cnt = 0;
            int sum = 0;
            for (IntWritable val: values) {
                sum += val.get();
                cnt += 1;
            }
            FloatWritable result = new FloatWritable((float) sum / (float) cnt);
            context.write(key,result);
        }
    }

    public static void main(String args[]) throws Exception {
        Configuration conf = new Configuration();
        conf.addResource("./core-site.xml");
        Job job = Job.getInstance(conf, "Score Average");
        job.setJarByClass(ScoreAverage.class);
        job.setMapperClass(ScoreAverage.TokenizerMapper.class);
        job.setReducerClass(ScoreAverage.IntSumReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FloatWritable.class);

        FileInputFormat.addInputPath(job, new Path("/scores.txt"));
        Path outputPath = new Path("/output");
        FileOutputFormat.setOutputPath(job, outputPath);
        outputPath.getFileSystem(conf).delete(outputPath, true);

        job.waitForCompletion(true);
    }
}
