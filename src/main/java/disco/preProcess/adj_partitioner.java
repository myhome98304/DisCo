package disco.preProcess;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class adj_partitioner extends Partitioner<Text, Text> {


	@Override
	public int getPartition(Text arg0, Text arg1, int arg2) {
		// TODO Auto-generated method stub
		return Integer.parseInt(arg0.toString())%arg2;
	}

}
