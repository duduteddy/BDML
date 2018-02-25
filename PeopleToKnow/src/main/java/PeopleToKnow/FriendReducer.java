package PeopleToKnow;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.StringUtils;
import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.Map.Entry;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;

public class FriendReducer extends Reducer<Text, Text, IntWritable, Text> {
    HashMap<String, Integer> recommendList;

    String[] pairStr;
//    int[] pair = new int[2];
    List<String> inputVal;
    StringBuffer suggestionList;
    StringBuffer tmp = new StringBuffer();
    String recList = new String();
    IntWritable outKey;

    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        recommendList = new HashMap<String, Integer>();
        suggestionList = new StringBuffer();
        tmp = new StringBuffer();
        inputVal = new ArrayList<String>();
        //read the inputvalues
        for(Text val : values){
            inputVal.add(val.toString());
        }


        for(String val : inputVal){
            pairStr = val.split(",");

            if(pairStr[1].equals("-1")){

                recommendList.put(pairStr[0],-1);
                //System.out.print((pairStr[0])+ "*****");
            }else if(pairStr[1].equals("1")){
                if(recommendList.containsKey(pairStr[0])){
                    if(recommendList.get(pairStr[0])!= -1){
                        recommendList.put(pairStr[0],recommendList.get(pairStr[0])+1);
                    }

                }
                else{

                    recommendList.put(pairStr[0],1);
                }

            }
        }

        HashMap list = new HashMap<String, Integer>();
        Iterator it = recommendList.entrySet().iterator();
        while (it.hasNext()){
            Map.Entry pair = (Map.Entry)it.next();
            if (Integer.parseInt(pair.getValue().toString())!= -1) {
                list.put(pair.getKey(),pair.getValue());
            }
        }

        //call sort procedure
        recList = sortFriends(list);
        outKey = new IntWritable(Integer.parseInt(key.toString()));
        context.write(outKey, new Text(recList));
    }





        public String sortFriends (HashMap a){
            Object[] recomm =  a.keySet().toArray();
            Object recommCount[] = a.values().toArray();
            //System.out.println("in sort, first value: " + recomm[0] + "\t" + recommCount[0]);

            //sort the arrays
            int i, j;
            Object temp, tempcount;
            StringBuffer returnlist = new StringBuffer();
            for(i=0; i<recommCount.length; i++) {
                for(j=0; j<recommCount.length-1 ; j++){
                    if(Integer.parseInt(recommCount[j].toString()) < Integer.parseInt(recommCount[j+1].toString()) ) {
                        //swap counts
                        tempcount = recommCount[j+1];
                        recommCount [j+1] = recommCount[j];
                        recommCount [j] = tempcount;

                        //swap the freinds
                        temp = recomm[j+1];
                        recomm [j+1] = recomm[j];
                        recomm [j] = temp;
                    }
                }
            }
            //put the friends in a string
            returnlist.append("\t");
            for(i=0; i<10 && i<recomm.length; i++) {
                returnlist.append(recomm[i].toString()).append(" ");
            }



            return returnlist.toString();
        }







}


