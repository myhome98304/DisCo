package old.disco.IncDimension;

import java.io.IOException;
import java.util.ArrayList;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.LineReader;

public class IncDiemnsion_mapper extends
		Mapper<LongWritable, Text, IntWritable, Text> {

	Text value = new Text();
	Text read = new Text();
	String job;

	int m, n;
	int[] row_permut;
	int[] col_permut;
	long[] rowSet;
	long[] colSet;
	long[][] subMatrix;
	int[] subM_change;
	long numof_maxShannon;
	double partSum_bef = 0;
	double partSum_aft;
	int num_machine;
	Configuration conf;
	int max_Shannon;
	int k, l;
	String outputPath;
	String subMatrix_s;
	IntWritable key = new IntWritable();
	IntWritable naive_key = new IntWritable();

	@Override
	public void setup(Context context) throws IOException {

		conf = context.getConfiguration();

		job = conf.get("job", "");
		outputPath = conf.get("outputPath", "");
		subMatrix_s = conf.get("subMatrix_String", "");
		max_Shannon = conf.getInt("max_Shannon", 0);
		num_machine = conf.getInt("num_machine", 1);

		k = conf.getInt("k", 0);
		l = conf.getInt("l", 0);
		m = conf.getInt("m", 0);
		n = conf.getInt("n", 0);

		row_permut = new int[m];
		col_permut = new int[n];
		rowSet = new long[k];
		colSet = new long[l];

		StringTokenizer st;

		int i;
		partSum_bef = Double.parseDouble(conf.get("partSum_bef", ""));

		for (LineReader lr : readLine(new Path(outputPath
				+ "/preProcess/r/assign"))) {
			while (lr.readLine(read) > 0) {

				st = new StringTokenizer(read.toString(), "\t");
				row_permut[Integer.parseInt(st.nextToken())] = Integer
						.parseInt(st.nextToken());
			}
		}

		for (LineReader lr : readLine(new Path(outputPath
				+ "/preProcess/c/assign"))) {
			while (lr.readLine(read) > 0) {
				st = new StringTokenizer(read.toString(), "\t");
				col_permut[Integer.parseInt(st.nextToken())] = Integer
						.parseInt(st.nextToken());
			}
		}

		i = 0;
		st = new StringTokenizer(conf.get("rowSet", ""), "[,] ");
		while (st.hasMoreTokens())
			rowSet[i++] = Long.parseLong(st.nextToken());

		i = 0;
		st = new StringTokenizer(conf.get("colSet", ""), "[,] ");
		while (st.hasMoreTokens())
			colSet[i++] = Long.parseLong(st.nextToken());

		String temp;
		i = 0;
		subMatrix = new long[k][l];
		st = new StringTokenizer(subMatrix_s, "{}\t ");

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

		int index;

		StringTokenizer st = new StringTokenizer(line.toString(), "\t ");

		index = Integer.parseInt(st.nextToken());

		line.set(index + "");

		if (job.equals("r")) {
			/* If given row is not in the target cluster, return */
			naive_key.set(row_permut[index] * num_machine
					+ (int) (Math.random() * num_machine));

			if (row_permut[index] != max_Shannon) {
				context.write(naive_key, line);
				return;
			}

			subM_change = new int[l];
			numof_maxShannon = rowSet[max_Shannon];

			/* Parse adjacency list */
			ArrayList<Integer> row_adj_list = new ArrayList<>();
			while (st.hasMoreTokens())
				row_adj_list.add(Integer.parseInt(st.nextToken()));

			partSum_aft = 0;

			/* split row by column cluster */
			for (int r_cur : row_adj_list)
				subM_change[col_permut[r_cur]]++;

			for (int c = 0; c < l; c++)
				partSum_aft += codeCost(numof_maxShannon - 1, colSet[c],
						subMatrix[max_Shannon][c] - subM_change[c]);

			partSum_aft /= numof_maxShannon - 1;

			/*
			 * If Delete a line decreases the cost, report to reducer Key :
			 * trash value 1 Value : row or column number + splitted adjacency
			 * list
			 */

			if (partSum_aft < partSum_bef) {
				key.set(k * num_machine + (int) (Math.random() * num_machine));

				context.write(key, line);
				context.write(key,
						new Text(1 + "\t" + arrToString(subM_change)));
			} else {
				context.write(naive_key, line);
			}

		} else {
			/* If given column is not in the target cluster, return */

			naive_key.set(col_permut[index] * num_machine
					+ (int) (Math.random() * num_machine));

			if (col_permut[index] != max_Shannon) {
				line.set(index + "");
				context.write(naive_key, line);
				return;
			}

			subM_change = new int[k];
			numof_maxShannon = colSet[max_Shannon];

			/* Parse adjacency list */
			ArrayList<Integer> col_adj_list = new ArrayList<>();
			while (st.hasMoreTokens())
				col_adj_list.add(Integer.parseInt(st.nextToken()));

			partSum_aft = 0;

			/* split column by column cluster */
			for (int c_cur : col_adj_list)
				subM_change[row_permut[c_cur]]++;

			for (int r = 0; r < k; r++)
				partSum_aft += codeCost(numof_maxShannon - 1, rowSet[r],
						subMatrix[r][max_Shannon] - subM_change[r]);

			partSum_aft /= numof_maxShannon - 1;

			if (partSum_aft < partSum_bef) {
				key.set(l * num_machine + (int) (Math.random() * num_machine));

				context.write(key, line);
				context.write(key,
						new Text(1 + "\t" + arrToString(subM_change)));
			} else {
				context.write(naive_key, line);
			}

		}

	}

	private static String arrToString(int[] arr) {
		String ret = "";
		for (int d : arr) {
			ret += d + " ";
		}
		return ret;
	}

	private static double codeCost(long m, long n, double total_weight) {
		// if (m == 0 || n == 0 || total_weight == 0 || total_weight == m * n)
		// return 0;
		double prob = (total_weight) / (m * n);

		return (m * n)
				* ((prob == 0 ? 0 : prob * Math.log(1 / prob)) + (1 - prob == 0 ? 0
						: (1 - prob) * Math.log(1 / (1 - prob))));
	}

	public ArrayList<LineReader> readLine(Path location) throws IOException {
		FileSystem fileSystem = FileSystem.get(location.toUri(), conf);
		CompressionCodecFactory factory = new CompressionCodecFactory(conf);
		FileStatus[] items = fileSystem.listStatus(location);
		ArrayList<LineReader> readLines = new ArrayList<>();
		for (FileStatus item : items) {

			// ignoring files like _SUCCESS
			if (item.getPath().getName().startsWith("_")) {
				continue;
			} else {
				readLines.add(new LineReader(fileSystem.open(item.getPath())));
			}
		}
		return readLines;
	}

}
