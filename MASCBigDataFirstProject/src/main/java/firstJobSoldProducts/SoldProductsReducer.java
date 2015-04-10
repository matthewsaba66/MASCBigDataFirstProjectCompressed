package firstJobSoldProducts;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class SoldProductsReducer extends
		Reducer<LongWritable, Text, Text, IntWritable> {

}
