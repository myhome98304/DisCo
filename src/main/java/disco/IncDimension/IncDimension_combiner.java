package disco.IncDimension;

import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class IncDimension_combiner extends
		Reducer<IntWritable, Text, IntWritable, Text> {
	String job;
	int k, l;
	
	Text val = new Text();
	Text assign = new Text();
	
	@Override
	public void setup(Context context) {
		Configuration conf = context.getConfiguration();
		job = conf.get("job", "");

		k = conf.getInt("k", 0);
		l = conf.getInt("l", 0);
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
		Iterator<Text> lines = values.iterator();
		StringTokenizer st1,st2;
		long num=0;
		int i;
		while (lines.hasNext()) {
			
			st1 = new StringTokenizer(lines.next().toString(), "\t");
			num+=Long.parseLong(st1.nextToken());
			
			st2 = new StringTokenizer(st1.nextToken()," ");
			
			i=0;
			while (st2.hasMoreTokens()) 
				subM_change[i++]+=Long.parseLong(st2.nextToken());
			
			ret+=st1.nextToken()+" ";
		}
		
		val.set(num+"\t"+arrToString(subM_change) + "\t"+ret);
		
		
		/* Report (row or column numbers to split) 
		 * + (values of decreased nozeros in each cluster after split) */ 
		context.write(key, val);
		
		
	}
	
	private static String arrToString(long[] arr){
		String ret="";
		for(long d:arr){
			ret+=d+" ";
		}
		return ret;
	}


}
