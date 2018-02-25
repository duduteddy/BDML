import java.io.IOException;
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
       extends Reducer<LongWritable,NullWritable,LongWritable,NullWritable> {

    Long  max = Long.MIN_VALUE;
    protected void reduce(LongWritable key, java.lang.Iterable<NullWritable> arg1,
                          org.apache.hadoop.mapreduce.Reducer<LongWritable,NullWritable,LongWritable,NullWritable>.Context arg2
                          ) throws java.io.IOException ,InterruptedException {
      final Long temp = key.get();
      if(temp>max){
        max = temp;
      }
    };

    protected void cleanup(org.apache.hadoop.mapreduce.Reducer<LongWritable,NullWritable,LongWritable,NullWritable>.Context context) throws java.io.IOException ,InterruptedException {
      context.write(new LongWritable(max), NullWritable.get());
    };
  }
