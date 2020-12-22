package distributed.system.cluster.management;

public interface OnElectionCallback {
	
	void onElectedToBeLeader();
	void onWorker();

}
