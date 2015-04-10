package secondJobVenditePrimoTrimestre;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class ThreeSellReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
	public void reduce(Text key, Iterable<IntWritable> values,
			Context context) throws IOException, InterruptedException {

		int sum = 0;

		for (IntWritable value : values) {
			sum += value.get();
		}

		context.write(key, new IntWritable(sum));
	}
}
