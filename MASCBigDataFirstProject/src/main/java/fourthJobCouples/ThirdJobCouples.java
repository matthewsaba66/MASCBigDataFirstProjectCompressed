package fourthJobCouples;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import thirdJobTenMostFreqSold.FirstJob;
import thirdJobTenMostFreqSold.FirstJobMapper;
import thirdJobTenMostFreqSold.FirstJobReducer;




public class ThirdJobCouples {

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException{
		
		Job job = Job.getInstance();
		job.setJobName("ThirdJobCouples");
		job.setJarByClass(ThirdJobCouples.class);

		job.setMapperClass(ThirdJobCouplesMapper.class);
		job.setReducerClass(ThirdJobCouplesReducer.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);

		job.waitForCompletion(true);
		
	}

}
