package fourthJobCouples;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;


public class SecondJobJoinMapperFile1 extends MapReduceBase 
implements Mapper<LongWritable, Text, Text, Text> {

	public void map(LongWritable key, Text value,
			OutputCollector<Text, Text> output, Reporter reporter)
					throws IOException {
		String line=value.toString();
		String[] words=line.split("\t");
		Text word = new Text(words[0]);
		Text number = new Text((words[1]));
		output.collect(word, number);
	}

}


