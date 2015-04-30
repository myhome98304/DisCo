package disco.preProcess;

import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Preprocess_combiner extends Reducer<IntWritable, Text, IntWritable, Text> {
	Text value = new Text();
	int num;
	StringTokenizer st;
	@Override
	public void reduce(IntWritable key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		
		String ret = "";
		num=0; 
		for(Text line : values){
			st = new StringTokenizer(line.toString(),"\t");
			num+=Integer.parseInt(st.nextToken());
			ret+=st.nextToken()+" ";
		}
		
	
		value.set(num+"\t"+ret);
		context.write(key, value);
	}

}