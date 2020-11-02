package MovieRecommend;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class Recommend {
    public static Configuration conf = new Configuration();

    public static void preprocess(){

    }

    public static void scaleScores(){

    }

    public static void getHotMovies() throws IOException, ClassNotFoundException, InterruptedException {
        Job job = Job.getInstance(conf, "Score Average");
        job.setJarByClass(AverageScore.class);
        job.setMapperClass(AverageScore.TokenizerMapper.class);
        job.setReducerClass(AverageScore.FloatReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FloatWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FloatWritable.class);

        FileInputFormat.addInputPath(job, new Path("/ratings.csv"));
        Path outputPath = new Path("/output");
        FileOutputFormat.setOutputPath(job, outputPath);
        outputPath.getFileSystem(conf).delete(outputPath, true);

        job.waitForCompletion(true);
    }

    public static void getSimilarUsers(){

    }

    public static void getCloseUsers(){

    }

    public static void getCloseMovies(){

    }

    public static void recommend(){

    }

    public static void main(String args[]) throws Exception {
        conf.addResource("./core-site.xml");
        preprocess();
        scaleScores();
        getHotMovies();
        getSimilarUsers();
        getCloseUsers();
        getCloseMovies();
        recommend();
    }
}
