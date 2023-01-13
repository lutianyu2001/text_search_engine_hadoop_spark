package Index;

import Index.util.MyArrayWritable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.log4j.BasicConfigurator;

import java.io.IOException;

public class IndexDriver {

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        if (args.length != 2) {
            System.err.println("Usage: IndexDriver <input path> <output path>");
            System.exit(-1);
        }

        BasicConfigurator.configure();

        Configuration conf = new Configuration();
        FileSystem hdfs = FileSystem.get(conf);

        Job job = Job.getInstance(conf, "IndexDriver");
        job.setJarByClass(IndexDriver.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setMapperClass(IndexMapper.class);
        job.setReducerClass(IndexReducer.class);

        // mapper output types
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        // reducer output types
        job.setOutputValueClass(Text.class);
        job.setOutputValueClass(MyArrayWritable.class);

        job.setInputFormatClass(TextInputFormat.class);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
