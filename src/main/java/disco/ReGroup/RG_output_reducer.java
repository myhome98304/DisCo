package disco.ReGroup;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

public class RG_output_reducer extends
		Reducer<IntWritable, Text, IntWritable, Text> {

	   private MultipleOutputs<IntWritable, Text> mos;
	    Text k = new Text();
	    IntWritable k1 =new IntWritable();
		@Override
		protected void cleanup(
				Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			mos.close();
		}
	@Override
	public void reduce(IntWritable key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {

		if(key.get()>-1)
			mos.write(key, values.iterator().next(),"subMatrix");
		
		
		
		else{
			
			mos.write(new IntWritable(-(key.get()+1)), values.iterator().next(),"assign");
		}
		
		
		
	}
	@Override
	protected void setup(
			Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		mos = new MultipleOutputs<IntWritable, Text>(context);
	}

}