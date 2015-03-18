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

	int m, n;
	int[] row_permut;
	int[] col_permut;
	long[] rowSet;
	long[] colSet;

	long[][] subMatrix;

	double partSum_bef = 0;
	int max_Shannon;
	int k, l;

	@Override
	public void setup(Context context) {

		Configuration conf = context.getConfiguration();

		job = conf.get("job", "");

		String subMatrix_s = conf.get("subMatrix_String", "");

		max_Shannon = conf.getInt("max_Shannon", 0);

		k = conf.getInt("k", 0);
		l = conf.getInt("l", 0);
		m = conf.getInt("m", 0);
		n = conf.getInt("n", 0);

		rowSet = new long[k];
		colSet = new long[l];

		partSum_bef = Double.parseDouble(conf.get("partSum_bef", ""));

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

		i = 0;
		st = new StringTokenizer(conf.get("rowSet", ""), "[,] ");
		while (st.hasMoreTokens())
			rowSet[i++] = Long.parseLong(st.nextToken());

		i = 0;
		st = new StringTokenizer(conf.get("colSet", ""), "[,] ");
		while (st.hasMoreTokens())
			colSet[i++] = Long.parseLong(st.nextToken());

		st = new StringTokenizer(subMatrix_s, "{}\t ");

		i = 0;
		String temp;

		subMatrix = new long[k][l];

		while (st.hasMoreElements()) {
			temp = st.nextToken();
			String[] cand = temp.split(",");
			for (int j = 0; j < l; j++) {
				subMatrix[i][j] = (int) Float.parseFloat(cand[j]);
			}
			i++;
		}
	}

	@Override
	public void map(LongWritable arg0, Text line, Context context)
			throws IOException, InterruptedException {

		int[] subM_change;
		double partSum_aft;
		int index;
		long numof_maxShannon;

		StringTokenizer st = new StringTokenizer(line.toString(), "\t ");

		index = Integer.parseInt(st.nextToken());

		if (job.equals("r")) {
			/* If given row is not in the target cluster, return */
			if (row_permut[index] != max_Shannon)
				return;

			subM_change = new int[l];
			numof_maxShannon = rowSet[max_Shannon];

			/* Parse adjacency list */
			while (st.hasMoreTokens()) {
				try {
					subM_change[col_permut[Integer.parseInt(st.nextToken())]]++;
				} catch (Exception e) {
					System.out.println("Out of Range");
				}

			}

			partSum_aft = 0;

			/* Calculate Cost of cluster after deleting given row */
			for (int c = 0; c < l; c++)
				partSum_aft += codeCost(numof_maxShannon - 1, colSet[c],
						subMatrix[max_Shannon][c] - subM_change[c]);

			partSum_aft /= numof_maxShannon - 1;

		} else {
			/* If given column is not in the target cluster, return */
			if (col_permut[index] != max_Shannon)
				return;

			subM_change = new int[k];
			numof_maxShannon = colSet[max_Shannon];

			/* Parse adjacency list */
			while (st.hasMoreTokens()) {
				try {
					subM_change[row_permut[Integer.parseInt(st.nextToken())]]++;
				} catch (Exception e) {
					System.out.println("Out of Range");
				}

			}

			partSum_aft = 0;
 
			for (int r = 0; r < k; r++)
				partSum_aft += codeCost(numof_maxShannon - 1, rowSet[r],
						subMatrix[r][max_Shannon] - subM_change[r]);

			partSum_aft /= numof_maxShannon - 1;

		}

		/*
		 * If Delete a line decreases the cost, report to reducer Key : trash
		 * value 1 Value : row or column number + splitted adjacency list
		 */
		if (partSum_aft < partSum_bef)
			context.write(new IntWritable(), new Text(1 + "\t"
					+ arrToString(subM_change) + "\t" + index));
		else
			return;

	}

	private static double codeCost(long m, long n, double total_weight) {
		// if (m == 0 || n == 0 || total_weight == 0 || total_weight == m * n)
		// return 0;
		double prob = (total_weight) / (m * n);

		return (m * n)
				* ((prob == 0 ? 0 : prob * Math.log(1 / prob)) + (1 - prob == 0 ? 0
						: (1 - prob) * Math.log(1 / (1 - prob))));
	}

	private static String arrToString(int[] arr) {
		String ret = "";
		for (int d : arr) {
			ret += d + " ";
		}
		return ret;
	}

}
