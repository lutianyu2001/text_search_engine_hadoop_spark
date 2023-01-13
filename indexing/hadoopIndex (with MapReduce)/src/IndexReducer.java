package Index;

import Index.util.MyArrayWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;

public class IndexReducer extends Reducer<Text, Text, Text, MyArrayWritable> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        ArrayList<Text> list = new ArrayList<>();
        for (Text filename: values) {
            if (!list.contains(filename)) {
                list.add(new Text(filename));
            }
        }
        context.write(key, new MyArrayWritable(Text.class, list.toArray(new Text[list.size()])));
    }
}
