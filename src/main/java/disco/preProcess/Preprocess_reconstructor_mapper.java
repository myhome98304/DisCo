package disco.preProcess;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Preprocess_reconstructor_mapper extends
		Mapper<IntWritable, Text, IntWritable, IntWritable> {
	String job, cluster, subM, adj;
	int sum;
	IntWritable val = new IntWritable();

	@Override
	protected void map(IntWritable key, Text value,
			Mapper<IntWritable, Text, IntWritable, IntWritable>.Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		StringTokenizer st = new StringTokenizer(value.toString(), "\t");
		job = st.nextToken();
		cluster = st.nextToken();
		key.set(Integer.parseInt(cluster));

		val.set(1);
		if (job.equals("r"))
			key.set(Integer.parseInt(cluster));

		else
			key.set(-Integer.parseInt(cluster) - 1);

		context.write(key, val);
	}
}
