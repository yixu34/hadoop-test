import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class SumNumbersMapper extends MapReduceBase
    implements Mapper<LongWritable, Text, IntWritable, IntWritable> {

    public void map(LongWritable key, Text value,
                    OutputCollector<IntWritable, IntWritable> output, Reporter reporter) throws IOException {
        String line = value.toString();
        int number = Integer.parseInt(line);
        output.collect(new IntWritable(0), new IntWritable(number));
    }
}
