package Index;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

public class IndexMapper extends Mapper<LongWritable, Text, Text, Text> {
    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        String line = value.toString();
        String filename =((FileSplit) context.getInputSplit()).getPath().getName();
        String[] words = line.split("\\s+|,|\\.|:"); // split line to words
        for (String word: words) {
            boolean flag = whetherWord(word);
            if(!flag || word.equals("")) continue;
            word = word.toLowerCase();
            context.write(new Text(word), new Text(filename));
        }
    }

    private static boolean whetherWord(String str) {
        // function for judging whether string str is a word
        boolean flag = true;
        for(int i = 0; i < str.length(); i++) {
            if((str.charAt(i) > 'z' || str.charAt(i) < 'a') &&
                    (str.charAt(i) > 'Z' || str.charAt(i) < 'A')){
                flag = false;
                break;
            }
        }
        return flag;
    }

}
