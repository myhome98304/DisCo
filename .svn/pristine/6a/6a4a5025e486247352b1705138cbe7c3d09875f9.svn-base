package cocluster.crAssos;

import java.util.ArrayList;

import cocluster.CoClustering;
public class CrossAssociation implements CoClustering {
	static long[][] subMatrix;
	static double[][] codeMatrix;
	static ArrayList<Long> rowSet;
	static ArrayList<Long> colSet;

	int k, l;

	public CrossAssociation(int k, int l) {
		rowSet = new ArrayList<>();
		colSet = new ArrayList<>();
		subMatrix = new long[1][1];
		codeMatrix = new double[1][1];
		this.k = k;
		this.l = l;
	}

	@Override
	public int SplitCluster(String job) {
		// TODO Auto-generated method stub
		int i, j;
		double temp;
		double cur_cost;

		/* Index, Size of cluster having maximun cost per row or column */
		int max_Shannon = 0;
		long cluster_size = 0;
		double partSum_bef = 0;

		if (job.equals("r")) {
			max_Shannon = 0;
			temp = 0;

			for (i = 0; i < k; i++) {

				cur_cost = 0;

				if (rowSet.get(i) != 0) {
					for (j = 0; j < l; j++) {
						cur_cost += codeMatrix[i][j];
					}

					cur_cost /= rowSet.get(i);
				}

				if (cur_cost > temp) {
					temp = cur_cost;
					max_Shannon = i;
					cluster_size = rowSet.get(i);
				}
			}

			partSum_bef = temp;

		} else if(job.equals("c")) {
			max_Shannon = 0;
			temp = 0;
			for (i = 0; i < l; i++) {

				cur_cost = 0;

				if (colSet.get(i) != 0) {
					for (j = 0; j < k; j++) {
						cur_cost += codeMatrix[j][i];
					}

					cur_cost /= colSet.get(i);
				}

				if (cur_cost > temp) {
					temp = cur_cost;
					max_Shannon = i;
					cluster_size = colSet.get(i);
				}
			}

			partSum_bef = temp;
		}
		else throw new Exception();
		return max_Shannon;
	}

	@Override
	public int RowStatistic() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int ColStatistic() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public void CombineStatistic() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public Exception NotProperJobException(String job) {
		// TODO Auto-generated method stub
		return Exce
	}
}
