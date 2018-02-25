import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.lang.*;
import java.util.*;


  public class FindNumbersMapper
       extends Mapper<LongWritable, Text, LongWritable, NullWritable>{


      Long max = Long.MIN_VALUE;


    public void map(LongWritable key, Text value, Context context
                    ) throws IOException, InterruptedException {


        final Long temp = Long.parseLong(value.toString());
      if(temp > max){
          max = temp;
      }
    }


      protected void cleanup(org.apache.hadoop.mapreduce.Mapper<LongWritable,Text,LongWritable, NullWritable>.Context context
        ) throws java.io.IOException ,InterruptedException {
          context.write(new LongWritable(max), NullWritable.get());
      };
  }
