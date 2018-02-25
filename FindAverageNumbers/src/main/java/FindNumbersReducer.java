import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

  public class FindNumbersReducer
       extends Reducer<Text,LongWritable,Text, LongWritable> {

    private Text ave = new Text("average");
    private LongWritable result = new LongWritable();
    protected void reduce(Text key ,Iterable<LongWritable> values,
                          Context context
                          ) throws IOException ,InterruptedException {

      long sum = 0;

      long count = 0;

      for (LongWritable val : values) {
        long v = val.get();
        sum += v;
        count++;
      }

      result.set( sum / count);// 计算平均成绩

      context.write(ave,result);

    }



  }
