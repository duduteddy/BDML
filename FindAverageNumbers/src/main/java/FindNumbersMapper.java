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
       extends Mapper<LongWritable, Text, Text, LongWritable>{





    public void map(LongWritable key, Text value, Context context
                    ) throws IOException, InterruptedException {
        Text sum = new Text("Average: ");
        final String[] splited = value.toString().split("\n");


        for(String nums : splited){
            long res = Long.parseLong(nums);
            context.write(sum,new LongWritable(res));
        }
    }


  }
