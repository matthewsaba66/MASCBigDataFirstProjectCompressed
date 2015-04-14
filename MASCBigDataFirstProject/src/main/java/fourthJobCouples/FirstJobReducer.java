package fourthJobCouples;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;



public class FirstJobReducer extends Reducer<Text, IntWritable, Text, IntWritable> {





	@Override
	protected void reduce(Text key, Iterable<IntWritable> values,
			Reducer<Text, IntWritable, Text, IntWritable>.Context context)
					throws IOException, InterruptedException {
		int sum = 0;

		for (IntWritable value : values) {
			sum += value.get();
		}
		context.write(new Text(key), 
				new IntWritable(sum));
	}


}



