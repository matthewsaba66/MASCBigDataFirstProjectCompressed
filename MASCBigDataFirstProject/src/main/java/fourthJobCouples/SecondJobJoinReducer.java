package fourthJobCouples;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class SecondJobJoinReducer extends MapReduceBase implements
		Reducer<K2, V2, K3, V3> {

	public void reduce(K2 key, Iterator<V2> values,
			OutputCollector<K3, V3> output, Reporter reporter)
			throws IOException {
		// TODO Auto-generated method stub
		
	}

}
