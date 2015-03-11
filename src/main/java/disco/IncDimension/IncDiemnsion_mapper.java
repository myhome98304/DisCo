package disco.IncDimension;

import java.io.IOException;
import java.util.ArrayList;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class IncDiemnsion_mapper extends
		Mapper<LongWritable, Text, IntWritable, Text> {
	IntWritable key = new IntWritable();
	Text value = new Text();
	String job;
	
	int m,n;
	int[] row_permut;
	int[] col_permut;
	long[] rowSet;
	long[] colSet;


	int max_Shannon;
	int k, l;

	@Override
	public void setup(Context context) {

		Configuration conf = context.getConfiguration();

		job = conf.get("job", "");

		max_Shannon = conf.getInt("max_Shannon", 0);

		k = conf.getInt("k", 0);
		l = conf.getInt("l", 0);
		m = conf.getInt("m", 0);
		n = conf.getInt("n", 0);
				
		rowSet = new long[k];
		colSet = new long[l];


		StringTokenizer st;

		int i;

		row_permut = new int[m];
		i = 0;
		
		st = new StringTokenizer(conf.get("row_permut", ""), " ");
		while (st.hasMoreElements())
			row_permut[i++] = Integer.parseInt(st.nextToken());

		col_permut = new int[n];
		i = 0;
		
		st = new StringTokenizer(conf.get("col_permut", ""), " ");
		while (st.hasMoreElements())
			col_permut[i++] = Integer.parseInt(st.nextToken());

		i=0;
		st = new StringTokenizer(conf.get("rowSet", ""), "[,] ");
		while (st.hasMoreTokens()) 
			rowSet[i++] = Long.parseLong(st.nextToken());
		

		i = 0;
		st = new StringTokenizer(conf.get("colSet", ""), "[,] ");
		while (st.hasMoreTokens()) 
			colSet[i++] = Long.parseLong(st.nextToken());
		

	}

	@Override
	public void map(LongWritable arg0, Text line, Context context)
			throws IOException, InterruptedException {

		int[] subM_change;
		double partSum_aft;
		int index;

		StringTokenizer st = new StringTokenizer(line.toString(), "\t ");

		index = Integer.parseInt(st.nextToken());

		if (job.equals("r")) {
			/* If given row is not in the target cluster, return */
			if (row_permut[index] != max_Shannon) 
				return;
			
			subM_change = new int[l];

			/* Parse adjacency list */
			ArrayList<Integer> row_adj_list = new ArrayList<>();
			while (st.hasMoreTokens()) 
				row_adj_list.add(Integer.parseInt(st.nextToken()));
			
			partSum_aft = 0;

			/* split row by column cluster */
			for (int r_cur : row_adj_list)
				subM_change[col_permut[r_cur]]++;


		} else {
			/* If given column is not in the target cluster, return */
			if (col_permut[index] != max_Shannon) 
				return;
			

			subM_change = new int[k];

			/* Parse adjacency list */
			ArrayList<Integer> col_adj_list = new ArrayList<>();
			while (st.hasMoreTokens()) 
				col_adj_list.add(Integer.parseInt(st.nextToken()));
			

			partSum_aft = 0;

			/* split column by column cluster */
			for (int c_cur : col_adj_list)
				subM_change[row_permut[c_cur]]++;

		}

		/*
		 * If Delete a line decreases the cost, report to reducer Key : trash
		 * value 1 Value : row or column number + splitted adjacency list
		 */
		if(Math.random()>=0.5)
			context.write(new IntWritable(), new Text(index + "\t"
					+ arrToString(subM_change)));
		
	}

	private static String arrToString(int[] arr) {
		String ret = "";
		for (int d : arr) {
			ret += d + " ";
		}
		return ret;
	}

}
