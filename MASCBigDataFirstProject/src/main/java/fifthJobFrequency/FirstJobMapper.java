package fifthJobFrequency;


import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;



public class FirstJobMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

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




