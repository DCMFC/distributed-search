package distributed.system;

import java.net.InetAddress;
import java.net.UnknownHostException;

import org.apache.zookeeper.KeeperException;

import distributed.system.cluster.management.OnElectionCallback;
import distributed.system.cluster.management.ServiceRegistry;
import distributed.system.networking.WebServer;
import distributed.system.search.SearchWorker;

public class OnElectionAction implements OnElectionCallback {
	
	private final ServiceRegistry serviceRegistry;
	private final int port;
	private WebServer webServer;
	
	public OnElectionAction(ServiceRegistry serviceRegistry, int port) {
		this.serviceRegistry = serviceRegistry;
		this.port = port;
	}

	public void onElectedToBeLeader() {
		try {
			serviceRegistry.unregisterFromCluster();
			serviceRegistry.registerForUpdates();
		} catch (KeeperException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}				
	}

	public void onWorker() {
		String currentServiceAddress;
		SearchWorker searchWorker = new SearchWorker();
		webServer = new WebServer(port, searchWorker);
		webServer.startServer();
		try {
			currentServiceAddress = String.format("http://%s:%d%s", 
					InetAddress.getLocalHost().getCanonicalHostName(), port, searchWorker.getEndpoint());
			serviceRegistry.registerToCluster(currentServiceAddress);
		} catch (UnknownHostException e) {
			e.printStackTrace();
		} catch (KeeperException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}	
		
	}
	
	

}
