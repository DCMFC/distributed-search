package distributed.system.networking;

public interface OnRequestCallback {
	
	byte[] handleRequest(byte[] requestPayload);
	
	String getEndpoint();

}
