package PeopleToKnow;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;


public class FriendMapper extends Mapper<LongWritable, Text, Text, Text> {
    Text userKey = new Text();
    Text suggestion = new Text();
    Text alreadyF = new Text();
    String[] lineSplit, friendSplit;
    int i ,j;


    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        lineSplit = value.toString().split("\t");
        if(lineSplit.length == 2){
            friendSplit = lineSplit[1].split(",");

            for(i = 0; i< friendSplit.length;i++){
                userKey.set(new Text(friendSplit[i]));
                for(j = 0; j< friendSplit.length; j++){
                    if(j == i){
                        continue;
                    }
                    suggestion.set(friendSplit[j] + ",1");
                    context.write(userKey, suggestion);
                }
                alreadyF.set(lineSplit[0] + ",-1");
                context.write(userKey, alreadyF);


            }
        }

    }
}
