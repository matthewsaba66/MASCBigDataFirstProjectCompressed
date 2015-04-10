package secondJobVenditePrimoTrimestre;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class FirstJob {

	public static void main(String[] args) throws Exception {

		Job job = Job.getInstance();
		job.setJobName("FirstJob");

		job.setJarByClass(FirstJob.class);
		
		job.setMapperClass(FirstJobMapper.class);
		job.setReducerClass(FirstJobReducer.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		job.waitForCompletion(true);
	}
}
