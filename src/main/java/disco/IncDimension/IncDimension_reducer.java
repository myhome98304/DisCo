package disco.IncDimension;

import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class IncDimension_reducer extends
		Reducer<IntWritable, Text, IntWritable, Text> {
	String job;
	int k, l;

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
		
		while (lines.hasNext()) {
			int i=0;
			st1 = new StringTokenizer(lines.next().toString(), "\t");
			ret+=" "+ st1.nextToken();
			st2 = new StringTokenizer(st1.nextToken()," ");
			while (st2.hasMoreTokens()) 
				subM_change[i++]+=Long.parseLong(st2.nextToken());
		}
		/* Report (row or column numbers to split) 
		 * + (values of decreased nozeros in each cluster after split) */ 
		context.write(key, new Text(ret+"\t"+arrToString(subM_change)));

	}
	private static String arrToString(long[] arr){
		String ret="";
		for(long d:arr){
			ret+=d+" ";
		}
		return ret;
	}


}