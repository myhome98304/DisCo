package disco.IncDimension;

import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

public class IncDimension_reducer extends
		Reducer<IntWritable, Text, IntWritable, Text> {
	String job;
	int k, l;
	MultipleOutputs<IntWritable, Text> mos;
	
	Text subM = new Text();
	Text assign = new Text();
	
	@Override
	public void setup(Context context) {
		Configuration conf = context.getConfiguration();
		job = conf.get("job", "");

		k = conf.getInt("k", 0);
		l = conf.getInt("l", 0);
		mos = new MultipleOutputs<IntWritable, Text>(context);
	}

	@Override
	protected void cleanup(
			Reducer<IntWritable, Text, IntWritable, Text>.Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		mos.close();
	}

	@Override
	public void reduce(IntWritable key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		long[] subM_change;
		
		if (job.equals("r")) {
			subM_change = new long[l];
		}
		else{
			subM_change = new long[k];
		}
			
		
		String ret = "";

		StringTokenizer st1,st2;
		int num=0;
		int i;
		
		for(Text line : values){
		
			st1 = new StringTokenizer(line.toString(), "\t");
			
			num+=Long.parseLong(st1.nextToken());
			
			
			st2 = new StringTokenizer(st1.nextToken()," ");
			
			i=0;
			while (st2.hasMoreTokens()) 
				subM_change[i++]+=Long.parseLong(st2.nextToken());
			
			ret+=st1.nextToken()+" ";
			
		}
		
		subM.set(num+"\t"+arrToString(subM_change));
		assign.set(ret);
		
		/* Report (row or column numbers to split) 
		 * + (values of decreased nozeros in each cluster after split) */ 
		mos.write(key, subM,"subMatrix");
		mos.write(key, assign, "assign");
		
	}
	
	private static String arrToString(long[] arr){
		String ret="";
		for(long d:arr){
			ret+=d+" ";
		}
		return ret;
	}


}
