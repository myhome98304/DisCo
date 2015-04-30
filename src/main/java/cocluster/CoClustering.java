package cocluster;

public interface CoClustering {
	int SplitCluster(String job);
	int RowStatistic();
	int ColStatistic();
	void CombineStatistic();
}
