package fourthJobCoupleRatio;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.PriorityQueue;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.lib.MultipleInputs;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import firstJobSoldProducts.SoldProducts;
import firstJobSoldProducts.SoldProducts.Mapper1;
import firstJobSoldProducts.SoldProducts.Reducer1;




public class CoupleRatio extends Configured implements Tool {

	public int run(String[] args) throws Exception {
		Path input = new Path(args[0]);
		Path temp1 = new Path("temp/temp1");
		Path temp2 = new Path("temp/temp2");
		Path temp3 = new Path("temp/temp3");
		Path output = new Path(args[1]);

		Job job1 = Job.getInstance();
		job1.setJobName("FirstJob");
		FileInputFormat.addInputPath(job1, input);
		FileOutputFormat.setOutputPath(job1, temp1);
		job1.setJarByClass(SoldProducts.class);
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
		FileInputFormat.setInputPaths(job2, input);
		FileOutputFormat.setOutputPath(job2, temp2);
		job2.setJarByClass(CoupleRatio.class);
		job2.setMapperClass(Mapper2.class);
		job2.setReducerClass(Reducer2.class);
		//job2.setInputFormatClass(KeyValueTextInputFormat.class);
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(IntWritable.class);
		job2.setNumReduceTasks(1);
		succ = job2.waitForCompletion(true);
		if (! succ) {
			System.out.println("Job2 failed, exiting");
			return -1;
		}

		Configuration conf = new Configuration();
		JobConf job3 = new JobConf(conf);
		//JobConf conf = new JobConf(SecondJobJoin.class);
		job3.setJobName("SecondJobJoin");
		//FileInputFormat.addInputPath(conf, new Path(args[0]));
		MultipleInputs.addInputPath(job3, temp1, TextInputFormat.class, Mapper3File1.class);
		MultipleInputs.addInputPath(job3, temp2, TextInputFormat.class, Mapper3File2.class);
		org.apache.hadoop.mapred.FileOutputFormat.setOutputPath(job3, temp3);
		//conf.setMapperClass(SecondJobJoinMapperFile1.class);
		job3.setReducerClass(Reducer3.class);
		job3.setOutputKeyClass(Text.class);
		job3.setOutputValueClass(Text.class);
		JobClient.runJob(job3);
		if (! succ) {
			System.out.println("Job3 failed, exiting");
			return -1;
		}

		Job job4 = Job.getInstance();
		job4.setJobName("SecondJob");
		FileInputFormat.setInputPaths(job4, temp3);
		FileOutputFormat.setOutputPath(job4, output);
		job4.setJarByClass(CoupleRatio.class);
		job4.setMapperClass(Mapper4.class);
		job4.setReducerClass(Reducer4.class);
		//job2.setInputFormatClass(KeyValueTextInputFormat.class);
		job4.setOutputKeyClass(Text.class);
		job4.setOutputValueClass(DoubleWritable.class);
		job4.setNumReduceTasks(1);
		succ = job4.waitForCompletion(true);
		if (! succ) {
			System.out.println("Job4 failed, exiting");
			return -1;
		}




		return 0;
	}
	public static void main(String[] args) throws Exception {
		if (args.length != 2) {
			System.out.println("Usage: fourthJobCoupleRatio/Coupleratio input/input.txt output/results");
			System.exit(-1);
		}
		int res = ToolRunner.run(new Configuration(), new CoupleRatio(), args);
		System.exit(res);
	}

	public static class Mapper2 extends Mapper<LongWritable, Text, Text, IntWritable> {

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
					/*ottiene anche coppie invertite*/
					String concReverse = ej + "," + ei;
					word.set(concReverse);
					context.write(word, one);
				}
			}
		}

	}

	public static class Reducer2 extends Reducer<Text, IntWritable, Text, IntWritable> {
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


	public static class Mapper3File1 extends MapReduceBase 
	implements org.apache.hadoop.mapred.Mapper<LongWritable, Text, Text, Text> {

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


	public static class Mapper3File2 extends MapReduceBase 
	implements org.apache.hadoop.mapred.Mapper<LongWritable, Text, Text, Text> {

		public void map(LongWritable key, Text value,
				OutputCollector<Text, Text> output, Reporter reporter)
						throws IOException {
			String line=value.toString();
			line = line.replace("\t", ",");
			String[] words=line.split(",");
			Text word = new Text(words[0]);
			Text number = new Text(words[1]+","+words[2]);
			output.collect(word, number);
		}

	}


	public static class Reducer3 extends MapReduceBase implements
	org.apache.hadoop.mapred.Reducer<Text, Text, Text, Text> {

		public void reduce(Text key, Iterator<Text> values,
				OutputCollector<Text, Text> output, Reporter reporter)
						throws IOException {
			String s = "";
			while(values.hasNext()){
				s = s.concat(values.next().toString()+" ");
			}
			String[] words=s.split(" ");
			if (isInt(words[0])){
				s = s.replace(words[0] + " ", "");
				output.collect(new Text(key +"," + words[0]), new Text(s));
			}
			else {
				s = s.replace(" " + words[words.length-1], "");
				output.collect(new Text(key + "," + words[words.length-1]), new Text(s));
			}
		}

		public static boolean isInt(String str) {

			try{
				int iCheck = Integer.parseInt(str);
				return true;
			}
			catch(NumberFormatException e) { return false; }
		}
	}

	public static class Mapper4 extends
	Mapper<LongWritable, Text,Text, DoubleWritable  > {

		@Override
		protected void map(LongWritable key, Text value,
				Mapper<LongWritable, Text,Text, DoubleWritable >.Context context)
						throws IOException, InterruptedException {

			String line = value.toString();
			line = line.replace("\t", ",");
			line = line.replace(" ", ",");
			String[] words = line.split(",");
			int k = 1;
			int j = 2;
			for (j = 2; j<words.length; j = j+2){
				Text word = new Text(words[0]+","+words[j]);
				DoubleWritable num = new DoubleWritable(Double.parseDouble(words[j+1])/Double.parseDouble(words[k]));
				context.write(word,num);
			}
		}
	}

	public static class Reducer4 extends Reducer<Text, DoubleWritable,   Text, DoubleWritable> {

		private class Pair {
			public String str;
			public Double count;

			public Pair( String str, Double count ) {
				this.str = str;
				this.count = count;
			}
		};
		private PriorityQueue<Pair> queue;

		private static final int TOP_K = 10;


		@Override
		protected void reduce(Text key, Iterable<DoubleWritable> values,
				Reducer<Text, DoubleWritable,Text, DoubleWritable >.Context context)
						throws IOException, InterruptedException {

			Double num = null;
			for (DoubleWritable value : values) {
				num = value.get();
			}
			queue.add(new Pair(key.toString(), num));
			if (queue.size() > TOP_K) {
				queue.remove();
			}
		}


		@Override
		protected void cleanup(
				Reducer< Text, DoubleWritable, Text, DoubleWritable >.Context context)
						throws IOException, InterruptedException {
			List<Pair> topKPairs = new ArrayList<Pair>();
			while (! queue.isEmpty()) {
				topKPairs.add(queue.remove());
			}
			for (int i = topKPairs.size() - 1; i >= 0; i--) {
				Pair topKPair = topKPairs.get(i);
				context.write(new Text(topKPair.str),new DoubleWritable( topKPair.count ));
			}
		}


		@Override
		protected void setup(
				Reducer< Text, DoubleWritable,Text, DoubleWritable >.Context context)
						throws IOException, InterruptedException {
			queue = new PriorityQueue<Pair>(TOP_K, new Comparator<Pair>() {
				public int compare(Pair p1, Pair p2) {
					return p1.count.compareTo(p2.count);
				}
			});
		}

	}

}
