package distributed.system.cluster.management;

import java.util.Collections;
import java.util.List;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

public class LeaderElection implements Watcher{
	
	private static final String ELECTION_NAMESPACE = "/election";
	private final ZooKeeper zookeeper;
	private String currentZnodeName;
	private final OnElectionCallback onElectionCallback;
	
	public LeaderElection(ZooKeeper zookeeper, OnElectionCallback onElectionCallback) {
		this.zookeeper = zookeeper;
		this.onElectionCallback = onElectionCallback;
	}
    
    public void volunteerForLeadership() throws KeeperException, InterruptedException {
    	String znodePrefix = ELECTION_NAMESPACE + "/c_";
    	String znodeFullPath = zookeeper.create(znodePrefix, new byte[] {}, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
    	
    	System.out.println("znode name "+ znodeFullPath);
    	this.currentZnodeName = znodeFullPath.replace(ELECTION_NAMESPACE + "/", "");
    }
    
    public void reelectLeader() throws KeeperException, InterruptedException {
    	Stat predecessorStat = null;
    	String predecessorZnodeName = "";
    	while(predecessorStat == null) {
    		List<String> children = zookeeper.getChildren(ELECTION_NAMESPACE, false);
        	
        	Collections.sort(children);
        	String smallestChild = children.get(0);
        	
        	if(smallestChild.equals(currentZnodeName)) {
        		System.out.println("I am the leader");
        		onElectionCallback.onElectedToBeLeader();
        		return;
        	}else {
        		System.out.println("I am not the leader");
        		int predecessorIndex = Collections.binarySearch(children, currentZnodeName) - 1;
        		predecessorZnodeName = children.get(predecessorIndex);
        		predecessorStat = zookeeper.exists(ELECTION_NAMESPACE  + "/" + predecessorZnodeName, this);
        	}
    	}
    	
    	onElectionCallback.onWorker();
    	System.out.println("Watching znode " + predecessorZnodeName);
    	System.out.println();
    }
    
	public void process(WatchedEvent event) {
		switch(event.getType()) {
			case NodeDeleted:
				try {
					reelectLeader();
				} catch (KeeperException e) {
					e.printStackTrace();
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
		default:
			break;				
		}
	}
}
