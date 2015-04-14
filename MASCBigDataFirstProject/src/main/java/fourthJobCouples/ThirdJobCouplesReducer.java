package fourthJobCouples;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;

import org.apache.hadoop.io.DoubleWritable;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;






public class ThirdJobCouplesReducer extends Reducer<Text, DoubleWritable,   Text, DoubleWritable> {
	
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
			
			context.write(new Text(topKPair.str), new DoubleWritable(topKPair.count));
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
