package thirdJobTenMostFreqSolds;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;




public class TenMostFreqSolds extends Configured implements Tool {

	public int run(String[] args) throws Exception {
		Path input = new Path(args[0]);
		Path temp1 = new Path("temp");
		Path output = new Path(args[1]);
		//Configuration conf = getConf();

		Job job1 = Job.getInstance();
		job1.setJobName("FirstJob");
		FileInputFormat.addInputPath(job1, input);
		FileOutputFormat.setOutputPath(job1, temp1);
		job1.setJarByClass(TenMostFreqSolds.class);
		job1.setMapperClass(Mapper1.class);
		//job1.setCombinerClass(Reducer1.class);
		job1.setReducerClass(Reducer1.class);
		//job1.setInputFormatClass(TextInputFormat.class);
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(IntWritable.class);
		boolean succ = job1.waitForCompletion(true);
		if (! succ) {
			System.out.println("Job1 failed, exiting");
			return -1;
		}

		Job job2 = Job.getInstance();
		job2.setJobName("SecondJob");
		FileInputFormat.setInputPaths(job2, temp1);
		FileOutputFormat.setOutputPath(job2, output);
		job2.setJarByClass(TenMostFreqSolds.class);
		job2.setMapperClass(Mapper2.class);
		job2.setReducerClass(Reducer2.class);
		//job2.setInputFormatClass(KeyValueTextInputFormat.class);
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(NullWritable.class);
		job2.setNumReduceTasks(1);
		job2.setSortComparatorClass(ReverseComparator.class);
		succ = job2.waitForCompletion(true);
		if (! succ) {
			System.out.println("Job2 failed, exiting");
			return -1;
		}

		return 0;
	}
	public static void main(String[] args) throws Exception {
		if (args.length != 2) {
			System.out.println("Usage: thirdJobTenMostFreqSolds/TenMostFreqSolds input/input.txt output/results");
			System.exit(-1);
		}
		int res = ToolRunner.run(new Configuration(), new TenMostFreqSolds(), args);
		System.exit(res);
	}
	
	public static class Mapper1 extends Mapper<LongWritable, Text, Text, IntWritable> {

		private static final IntWritable one = new IntWritable(1);
		private Text word = new Text();

		@Override
		protected void map(LongWritable key, Text value,
				Mapper<LongWritable, Text, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
			
			String line = value.toString();
			String regex = "(19|20)[0-9][0-9][--.]([0-9]|[1][0-2])[--.]([0-9]|1[0-9]|2[0-9]|3[0-1]),";
			line = line.replaceAll(regex, "");
			
			StringTokenizer tokenizer = new StringTokenizer(line, ",");
			
			List<String> list = new ArrayList<String>();
			while (tokenizer.hasMoreTokens()) {
				list.add(tokenizer.nextToken());
			}
			
			for (int i = 0; i < list.size(); i++) {
				   for (int j = i+1; j < list.size(); j++) {
				       String ei = list.get( i);
				       String ej = list.get( j);
				       String conc = ei + "," + ej;
				       word.set(conc);
				       context.write(word, one);
				   }
				}
			
			
		}

	}
	
	public static class Reducer1 extends Reducer<Text, IntWritable, Text, IntWritable> {


		private class Pair {
			public String str;
			public Integer count;

			public Pair(String str, Integer count) {
				this.str = str;
				this.count = count;
			}
		};
		private PriorityQueue<Pair> queue;

		private static final int TOP_K = 10;


		@Override
		protected void reduce(Text key, Iterable<IntWritable> values,
				Reducer<Text, IntWritable, Text, IntWritable>.Context context)
						throws IOException, InterruptedException {
			int sum = 0;

			for (IntWritable value : values) {
				sum += value.get();
			}
			queue.add(new Pair(key.toString(), sum));
			if (queue.size() > TOP_K) {
				queue.remove();
			}
		}




		@Override
		protected void cleanup(
				Reducer<Text, IntWritable, Text, IntWritable>.Context context)
						throws IOException, InterruptedException {
			List<Pair> topKPairs = new ArrayList<Pair>();
			while (! queue.isEmpty()) {
				topKPairs.add(queue.remove());
			}
			for (int i = topKPairs.size() - 1; i >= 0; i--) {
				Pair topKPair = topKPairs.get(i);
				
				context.write(new Text(topKPair.str), 
						new IntWritable(topKPair.count));
			}
		}

		@Override
		protected void setup(
				Reducer<Text, IntWritable, Text, IntWritable>.Context context)
						throws IOException, InterruptedException {
			queue = new PriorityQueue<Pair>(TOP_K, new Comparator<Pair>() {
				public int compare(Pair p1, Pair p2) {
					return p1.count.compareTo(p2.count);
				}
			});
		}


	}
	
	
	public static class Mapper2 extends Mapper<LongWritable, Text, Text, NullWritable> {

		@Override
		protected void map(LongWritable key, Text value,
				Mapper<LongWritable, Text, Text, NullWritable>.Context context)
				throws IOException, InterruptedException {
			String line = value.toString();
			
			line = line.replace("\t", ",");
			//line = line.replace(",", "");
			StringTokenizer tokenizer = new StringTokenizer(line, ",");
			String x1 = tokenizer.nextToken();
			String x2 = tokenizer.nextToken();
			String num = tokenizer.nextToken();
			line = num + "," + x1 + "," + x2;
			
			
			context.write(new Text(line), NullWritable.get());	
			}
			
	}
	
	public static class Reducer2 extends Reducer<Text, Text, Text, NullWritable>{

		@Override
		protected void reduce(Text key, Iterable<Text> values,
				Reducer<Text, Text, Text, NullWritable>.Context context)
				throws IOException, InterruptedException {
			
			String line = key.toString();
			//StringTokenizer tokenizer = new StringTokenizer(line, ",");
			String[] strings = line.split(",");
			String newLine = strings[1] + "," + strings[2] + "," + strings[0];
			context.write(new Text(newLine), NullWritable.get());
			
		}
		
	}
}
