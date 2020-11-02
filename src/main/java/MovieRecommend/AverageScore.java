package MovieRecommend;

/* Scale分数，并统计最热门的电影 */

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class AverageScore {

    public static class TokenizerMapper extends Mapper<Object, Text, Text, FloatWritable> {
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] str = value.toString().split(",");
            context.write(new Text(str[1]), new FloatWritable(Float.parseFloat(str[2])));
        }
    }

    public static class FloatReducer extends Reducer<Text, FloatWritable, Text, FloatWritable> {
        public void reduce(Text key, Iterable<FloatWritable> values, Context context) throws IOException, InterruptedException {
            int cnt = 0;
            float sum = 0;
            for (FloatWritable val : values) {
                sum += val.get();
                cnt += 1;
            }
            FloatWritable result = new FloatWritable((float) sum / (float) cnt);
            context.write(key, result);
        }
    }
}


