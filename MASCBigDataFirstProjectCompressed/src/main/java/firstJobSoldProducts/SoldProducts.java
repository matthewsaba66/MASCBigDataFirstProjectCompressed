package firstJobSoldProducts;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;





public class SoldProducts extends Configured implements Tool {

	public int run(String[] args) throws Exception {
		Path input = new Path(args[0]);
		Path output = new Path(args[1]);
		//Configuration conf = getConf();

		Job job1 = Job.getInstance();
		job1.setJobName("SoldProducts");
		job1.setJarByClass(SoldProducts.class);
		FileInputFormat.addInputPath(job1, input);
		FileOutputFormat.setOutputPath(job1, output);
		job1.setMapperClass(Mapper1.class);
		job1.setReducerClass(Reducer1.class);
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(IntWritable.class);
		boolean succ = job1.waitForCompletion(true);
		if (! succ) {
			System.out.println("Job1 failed, exiting");
			return -1;
		}


		return 0;

	}

	public static void main(String[] args) throws Exception {
		if (args.length != 2) {
			System.out.println("Usage: firstJobsoldProducts/SoldProducts input/esempio.txt output/result");
			System.exit(-1);
		}
		int res = ToolRunner.run(new Configuration(), new SoldProducts(), args);
		System.exit(res);
	}

	public static class Mapper1 extends
	Mapper<LongWritable, Text, Text, IntWritable> {

		private static final IntWritable one = new IntWritable(1);
		private Text word = new Text();

		@Override
		protected void map(LongWritable key, Text value, 
				Context context) throws IOException, InterruptedException {
			String line = value.toString();
			String regex = "(19|20)[0-9][0-9][--.]([0-9]|[1][0-2])[--.]([0-9]|1[0-9]|2[0-9]|3[0-1]),";
			line = line.replaceAll(regex, "");

			StringTokenizer tokenizer = new StringTokenizer(line, ",");

			while (tokenizer.hasMoreTokens()) {
				word.set(tokenizer.nextToken());
				context.write(word, one);
			}

		}
	}

	public static class Reducer1 extends
	Reducer<Text, IntWritable, Text, IntWritable> {


		@Override
		protected void reduce(Text key, Iterable<IntWritable> values, 
				Context context) throws IOException, InterruptedException {

			int sum = 0;

			for (IntWritable value : values) {
				sum += value.get();
			}

			context.write(key, new IntWritable(sum));

		}


	}
}


