package fourthJobCouples;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.lib.MultipleInputs;



public class SecondJobJoin {
	public static void main(String[] args) throws IOException{
		
		JobConf conf = new JobConf(SecondJobJoin.class);
        conf.setJobName("SecondJobJoin");
        
        FileInputFormat.addInputPath(conf, new Path(args[0]));
        MultipleInputs.addInputPath(conf, new Path(args[0]), TextInputFormat.class, SecondJobJoinMapperFile1.class);
        MultipleInputs.addInputPath(conf, new Path(args[1]), TextInputFormat.class, SecondJobJoinMapperFile2.class);
        FileOutputFormat.setOutputPath(conf, new Path(args[2]));
        
        //conf.setMapperClass(SecondJobJoinMapperFile1.class);
        conf.setReducerClass(SecondJobJoinReducer.class);
        
        conf.setMapOutputKeyClass(Text.class);
        conf.setMapOutputValueClass(Text.class);
        
        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(Text.class);
        
        JobClient.runJob(conf);
        
		
	}

}
