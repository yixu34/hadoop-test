import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;

public class SumNumbers {
    public static void main(String[] args) throws IOException {
        if (args.length != 2) {
            System.err.println("Usage: SumNumbers <input path> <output path>");
            System.exit(1);
        }

        JobConf conf = new JobConf(SumNumbers.class);
        conf.setJobName("Sum numbers");

        FileInputFormat.addInputPath(conf, new Path(args[0]));
        FileOutputFormat.setOutputPath(conf, new Path(args[1]));

        conf.setMapperClass(SumNumbersMapper.class);
        conf.setReducerClass(SumNumbersReducer.class);

        conf.setOutputKeyClass(IntWritable.class);
        conf.setOutputValueClass(IntWritable.class);

        JobClient.runJob(conf);
    }
}
