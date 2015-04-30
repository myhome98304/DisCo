package old.disco.IncDimension;

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
	int cur;
	int cluster;
	int num_machine;
	int index;
	MultipleOutputs<IntWritable, Text> mos;
	IntWritable ret_key = new IntWritable();
	Text ret_value = new Text();
	Text current;
	int ret;
	@Override
	public void setup(Context context) {
		Configuration conf = context.getConfiguration();
		job = conf.get("job", "");

		k = conf.getInt("k", 0);
		l = conf.getInt("l", 0);
		num_machine = conf.getInt("num_machine", 1);
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
		
		cluster = (key.get()-(key.get()%num_machine))/num_machine;
		cur = job.equals("r")?k:l;
		if(cluster !=cur){
			ret_value.set(cluster+"");
			for(Text line : values){
				ret_key.set(Integer.parseInt(line.toString()));
				mos.write("assign", ret_key, ret_value, "assign/assign");
			}
			return;	
		}
		else{
			

			if (job.equals("r")) {
				subM_change = new long[l];
			}
			else{
				subM_change = new long[k];
			}
			
			ret =0;
			
			StringTokenizer st1,st2;
			ret_value.set(cluster+"");
			for(Text line : values){	
				try{
					index = Integer.parseInt(line.toString());
					ret_key.set(index);
					mos.write("assign", ret_key, ret_value, "assign/assign");
				}
				catch(Exception e){
					int i=0;
					st1 = new StringTokenizer(line.toString(), "\t");
					ret+=+ Integer.parseInt(st1.nextToken());
					st2 = new StringTokenizer(st1.nextToken()," ");
					while (st2.hasMoreTokens()) 
						subM_change[i++]+=Long.parseLong(st2.nextToken());	
				}
				
			}
			/* Report (row or column numbers to split) 
			 * + (values of decreased nozeros in each cluster after split) */
			key.set(ret);
			mos.write("subM", key, new Text(arrToString(subM_change)), "subM/subM");	
		}
		

	}
	private static String arrToString(long[] arr){
		String ret="";
		for(long d:arr){
			ret+=d+" ";
		}
		return ret;
	}


}
