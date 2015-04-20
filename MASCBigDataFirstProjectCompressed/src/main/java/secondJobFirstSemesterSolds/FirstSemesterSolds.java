package secondJobFirstSemesterSolds;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
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



public class FirstSemesterSolds extends Configured implements Tool {

	public int run(String[] args) throws Exception {
		Path input = new Path(args[0]);
		Path temp1 = new Path("temp");
		Path output = new Path(args[1]);
		//Configuration conf = getConf();

		Job job1 = Job.getInstance();
		job1.setJobName("FirstJob");
		FileInputFormat.addInputPath(job1, input);
		FileOutputFormat.setOutputPath(job1, temp1);
		job1.setJarByClass(FirstSemesterSolds.class);
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
		job2.setJarByClass(FirstSemesterSolds.class);
		job2.setMapperClass(Mapper2.class);
		job2.setReducerClass(Reducer2.class);
		//job2.setInputFormatClass(KeyValueTextInputFormat.class);
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(Text.class);
		job2.setNumReduceTasks(1);
		succ = job2.waitForCompletion(true);
		if (! succ) {
			System.out.println("Job2 failed, exiting");
			return -1;
		}

		return 0;

	}


	public static void main(String[] args) throws Exception {
		if (args.length != 2) {
			System.out.println("Usage: secondJobFirstSemesterSolds/FirstSemesterSolds input/input.txt output/results");
			System.exit(-1);
		}
		int res = ToolRunner.run(new Configuration(), new FirstSemesterSolds(), args);
		System.exit(res);
	}


	public static class Mapper1 extends Mapper<LongWritable, Text, Text, IntWritable> {

		private Text word = new Text();
		private static final IntWritable one = new IntWritable(1);

		public void map(LongWritable key, Text value,
				Mapper<LongWritable, Text, Text, IntWritable>.Context context)
						throws IOException, InterruptedException {

			String line = value.toString();
			String date = "";
			String product = "";
			if (line.charAt(6)=='-' && (line.charAt(5)=='1' || line.charAt(5)=='2' 
					|| line.charAt(5)=='3') && line.substring(0, 4).equals("2015"))	{
				date = line.charAt(5)+ "/" +line.substring(0,4);
				if (line.charAt(8)==','){
					product = line.substring(9);
				}
				else {
					product = line.substring(10);
				}

			}
			StringTokenizer tokenizer = new StringTokenizer(product,",");

			while (tokenizer.hasMoreTokens()) {
				word.set(tokenizer.nextToken());
				Text data = new Text(word.toString()+" "+date);

				context.write(data, one);
			}

		}

	}

	public static class Reducer1 extends Reducer<Text, IntWritable, Text, IntWritable> {
		public void reduce(Text key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {

			int sum = 0;
			for (IntWritable value : values) {
				sum += value.get();
			}
			context.write(key, new IntWritable(sum));
		}
	}
	
	
	public static class Mapper2 extends Mapper<LongWritable, Text, Text, Text> {
		private Text word = new Text();
		@Override
		protected void map(LongWritable key, Text value,
				Mapper<LongWritable, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			
			String line = value.toString();
			
			line = line.replace("\t", ":");
			
			StringTokenizer tokenizer = new StringTokenizer(line," ");
			word.set(tokenizer.nextToken());
			
			context.write(word, new Text(tokenizer.nextToken()));
			//context.write(new Text("eseguito map"), new Text("eseguito Map"));
			}
				
		

	}

	
	public static class Reducer2 extends Reducer<Text, Text, Text, Text> {

		public void reduce(Text key, Iterable<Text> values,
				Context context) throws IOException, InterruptedException {
			
			String  trim = "";

			for (Text value : values) {
				trim = trim.concat(" "+value.toString());
			}
			StringTokenizer tokenizer = new StringTokenizer(trim," ");
			List<String> list = new ArrayList<String>();
			while (tokenizer.hasMoreTokens()){
				list.add(tokenizer.nextToken());
				
			}
			Collections.sort(list);
			
			String value = "";
			for (String s : list)
				value = value.concat(s + " ");

			context.write(key, new Text(value));
			
		}
		

	}
}
