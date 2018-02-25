package PeopleToKnow;

import java.io.IOException;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
public class FriendToKnowDriver {

    public static void main(String ar[]) throws IOException, InterruptedException, ClassNotFoundException {


        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "PeopleToKnow");

        job.setJarByClass(FriendToKnowDriver.class);
        job.setMapperClass(FriendMapper.class);
        job.setReducerClass(FriendReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(ar[0]));
        FileOutputFormat.setOutputPath(job, new Path(ar[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
