package fifthJobFrequence;

import java.io.IOException;
import java.util.ArrayList;
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



public class Frequence extends Configured implements Tool {


	public int run(String[] args) throws Exception {
		Path input = new Path(args[0]);
		Path output = new Path(args[1]);
		//Configuration conf = getConf();

		Job job1 = Job.getInstance();
		job1.setJobName("FirstJob");
		FileInputFormat.addInputPath(job1, input);
		FileOutputFormat.setOutputPath(job1, output);
		job1.setJarByClass(Frequence.class);
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
		return 0;

	}


	public static void main(String[] args) throws Exception {
		if (args.length != 2) {
			System.out.println("Usage: fifthJobFrequence/Frequence input/input.txt output/results");
			System.exit(-1);
		}
		int res = ToolRunner.run(new Configuration(), new Frequence(), args);
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
			int size = tokenizer.countTokens();

			if (size == 2){
				calcolaCoppie(line, context);
			}
			else if (size == 3){
				calcolaCoppie(line, context);
				calcolaTriple(line, context);
			}
			else if (size == 4){
				calcolaCoppie(line, context);
				calcolaTriple(line, context);
				calcolaQuadruple(line, context);
			}
			else if (size >= 5){
				calcolaCoppie(line, context);
				calcolaTriple(line, context);
				calcolaQuadruple(line, context);
				calcolaQuintuple(line, context);
			}
		}

		private void calcolaQuintuple(String line,
				Mapper<LongWritable, Text, Text, IntWritable>.Context context) throws IOException, InterruptedException {
			StringTokenizer tokenizer = new StringTokenizer(line, ",");

			List<String> list = new ArrayList<String>();
			while (tokenizer.hasMoreTokens()) {
				list.add(tokenizer.nextToken());
			}

			for (int i = 0; i < list.size(); i++) {
				for (int j = i+1; j < list.size(); j++) {
					for (int k = j+1; k < list.size(); k++){
						for(int l = k+1; l < list.size(); l++){
							for (int m = l+1; m < list.size(); m++){
								String ei = list.get( i);
								String ej = list.get( j);
								String ek = list.get( k);
								String el = list.get( l);
								String em = list.get( m);
								String conc = ei + "," + ej + "," + ek + "," + el + "," + em;
								word.set("5: " + conc);
								context.write(word, one);
							}
						}
					}
				}
			}
		}

		private void calcolaQuadruple(String line,
				Mapper<LongWritable, Text, Text, IntWritable>.Context context) throws IOException, InterruptedException {

			StringTokenizer tokenizer = new StringTokenizer(line, ",");

			List<String> list = new ArrayList<String>();
			while (tokenizer.hasMoreTokens()) {
				list.add(tokenizer.nextToken());
			}

			for (int i = 0; i < list.size(); i++) {
				for (int j = i+1; j < list.size(); j++) {
					for (int k = j+1; k < list.size(); k++){
						for(int l = k+1; l < list.size(); l++){
							String ei = list.get( i);
							String ej = list.get( j);
							String ek = list.get( k);
							String el = list.get( l);
							String conc = ei + "," + ej + "," + ek + "," + el;
							word.set("4: " + conc);
							context.write(word, one);
						}
					}
				}
			}
		}

		private void calcolaTriple(String line,
				Mapper<LongWritable, Text, Text, IntWritable>.Context context) throws IOException, InterruptedException {

			StringTokenizer tokenizer = new StringTokenizer(line, ",");

			List<String> list = new ArrayList<String>();
			while (tokenizer.hasMoreTokens()) {
				list.add(tokenizer.nextToken());
			}

			for (int i = 0; i < list.size(); i++) {
				for (int j = i+1; j < list.size(); j++) {
					for (int k = j+1; k < list.size(); k++){
						String ei = list.get( i);
						String ej = list.get( j);
						String ek = list.get( k);
						String conc = ei + "," + ej + "," + ek;
						word.set("3: " + conc);
						context.write(word, one);
					}
				}
			}


		}

		private void calcolaCoppie(String line, Mapper<LongWritable, Text, Text, IntWritable>.Context context) throws IOException, InterruptedException {

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
					word.set("2: " + conc);
					context.write(word, one);
				}
			}
		}
	}
	
	public static class Reducer1 extends Reducer<Text, IntWritable, Text, IntWritable> {

		@Override
		protected void reduce(Text key, Iterable<IntWritable> values,
				Reducer<Text, IntWritable, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
			int sum = 0;

			for (IntWritable value : values) {
				sum += value.get();
			}
			context.write(new Text(key.toString()), new IntWritable(sum));
		}

	}

}


