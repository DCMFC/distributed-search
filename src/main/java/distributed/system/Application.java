package distributed.system;


import java.io.IOException;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import distributed.system.cluster.management.LeaderElection;
import distributed.system.cluster.management.ServiceRegistry;

public class Application implements Watcher{
	
	private static final String ZOOKEEPER_ADDRESS = "localhost:2181";
	private static final int SESSION_TIMEOUT = 3000;
	private static final int DEFAULT_PORT = 8080;
	private ZooKeeper zookeeper;
	
	public static void main( String[] args ) throws IOException, InterruptedException, KeeperException{
    	int currentServerPort = args.length == 1 ? Integer.parseInt(args[0]) : DEFAULT_PORT; 
		Application application = new Application();
		ZooKeeper zookeeper = application.connectToZookeeper();
		
		ServiceRegistry workersServiceRegistry = new ServiceRegistry(zookeeper, ServiceRegistry.WORKERS_REGISTRY_ZNODE);
		ServiceRegistry coordenatorsServiceRegistry = new ServiceRegistry(zookeeper, ServiceRegistry.COORDINATORS_REGISTRY_ZNODE);
    			
		OnElectionAction onElectionAction = new OnElectionAction(workersServiceRegistry, coordenatorsServiceRegistry, currentServerPort);
		
    	LeaderElection leaderElection = new LeaderElection(zookeeper, onElectionAction);
    	leaderElection.volunteerForLeadership();
    	leaderElection.reelectLeader();
  
    	application.run();
    	application.close();
    	System.out.println("Disconnected from Zookeeper, exiting application.");
    }
	
	public ZooKeeper connectToZookeeper() throws IOException{
    	this.zookeeper = new ZooKeeper(ZOOKEEPER_ADDRESS, SESSION_TIMEOUT, this);
    	return zookeeper;
    }
    
    public void run() throws InterruptedException {
    	synchronized (zookeeper) {
    		zookeeper.wait();
    	}
    }

    public void close() throws InterruptedException {
    	zookeeper.close();
    }
	
    public void process(WatchedEvent event) {
		switch(event.getType()) {
			case None:
				if(event.getState() == Event.KeeperState.SyncConnected) {
					System.out.println("Successfully connected to ZooKeeper");
				}else {
					synchronized(zookeeper) {
						System.out.println("Disconnected from Zookeeper event");
						zookeeper.notifyAll();
					}
				}
				break;
		default:
			break;			
		}
	}
}