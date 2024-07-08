

// -----( IS Java Code Template v1.2

import com.wm.data.*;
import com.wm.util.Values;
import com.wm.app.b2b.server.Service;
import com.wm.app.b2b.server.ServiceException;
// --- <<IS-START-IMPORTS>> ---
import com.pcbsys.nirvana.nAdminAPI.*;
import com.pcbsys.nirvana.client.*;
import java.util.*;
// --- <<IS-END-IMPORTS>> ---

public final class services

{
	// ---( internal utility methods )---

	final static services _instance = new services();

	static services _newInstance() { return new services(); }

	static services _cast(Object o) { return (services)o; }

	// ---( server methods )---




	public static final void allMetricsData (IData pipeline)
        throws ServiceException
	{
		// --- <<IS-START(allMetricsData)>> ---
		// @sigtype java 3.5
		// [i] field:0:required hostName
		// [i] field:0:required portNumber
		// [o] field:0:required realmProviderURL
		// [o] record:1:required commonRealmMetrics
		// [o] - field:0:required channelSize
		// [o] - field:0:required queueSize
		// [o] - field:0:required published
		// [o] - field:0:required consumed
		// [o] - field:0:required totalConnections
		// [o] - field:0:required currentConnections
		// [o] - field:0:required totalMemory
		// [o] - field:0:required freeMemory
		// [o] - field:0:required usedMemory
		// [o] - field:0:required totalDirectMemory
		// [o] - field:0:required totalNodes
		// [o] - field:0:required heapPercentage
		// [o] - field:0:required clusterInfo
		// [o] - field:0:required clusterState
		// [o] record:1:required queueMetrics
		// [o] - field:0:required name
		// [o] - field:0:required queueLength
		// [o] - field:0:required numEventsPublished
		// [o] - field:0:required lastEventTime
		// [o] - field:0:required type
		// [o] - field:0:required mode
		// [o] - field:0:required totalPubished
		// [o] - field:0:required totalConsumed
		// [o] - field:0:required lastEventId
		// [o] - field:0:required currentConnections
		// [o] - object:0:required totalConnections
		// [o] - field:0:required publishRate
		// [o] - field:0:required consumedRate
		// [o] - field:0:required connectionRate
		// [o] - field:0:required fanoutTime
		// [o] - field:0:required usedSpaceKb
		// [o] record:1:required durableMetrics
		// [o] - field:0:required name
		// [o] - field:0:required queueLength
		// [o] - field:0:required numEventsPublished
		// [o] - field:0:required type
		// [o] - field:0:required mode
		// [o] - field:0:required totalPublished
		// [o] - field:0:required totalConsumed
		// [o] - field:0:required lastEventId
		// [o] - field:0:required currentConnections
		// [o] - field:0:required totalConnections
		// [o] - field:0:required publishRate
		// [o] - field:0:required consumedRate
		// [o] - field:0:required connectionRate
		// [o] - field:0:required fanoutTime
		// [o] - field:0:required usedSpaceKb
		// [o] - field:0:required lastEventTime
		// [o] record:0:required durableMetrics
		// [o] record:1:required channelMetrics
		// [o] - field:0:required channelName
		// [o] - object:0:required queueLength
		// [o] - field:0:required numEventsPublished
		// [o] - field:0:required type
		// [o] - field:0:required mode
		// [o] - object:0:required totalPublished
		// [o] - object:0:required totalConsumed
		// [o] - object:0:required lastEventId
		// [o] - object:0:required currentConnections
		// [o] - object:0:required totalConnections
		// [o] - object:0:required publishRate
		// [o] - object:0:required consumeRate
		// [o] - object:0:required connectionRate
		// [o] - object:0:required fanoutTime
		// [o] - object:0:required usedSpaceKb
		// --- <<IS-START(allMetrics)>> ---
		// @sigtype java 3.5
		// [i] field:0:required hostName
		// [i] field:0:required portNumber
		// [i] field:0:required realmProviderURL
		// [i] object:1:required assetMetrics
		// [o] field:0:required realmProviderURL
		// [o] record:1:required assetMetrics
		// [o] - object:1:required name
		// [o] - object:0:required type
		// [o] - object:0:required mode
		// [o] - object:0:required queueDepth
		// [o] - object:0:required eventId
		// [o] - object:0:required published
		// [o] - object:0:required consumed
		// [o] - object:0:required totalConnection
		// [o] - object:0:required currentConn
		// [o] - object:0:required publishRate
		// [o] - object:0:required consumeRate
		// [o] - object:0:required connectionRate
		// [o] - object:0:required fanoutTime
		// [o] - object:0:required usedSpaceKb
		// [o] record:1:required realmMetrics
		// [o] - object:0:required clusterState
		// [o] - object:0:required clusterInfo
		// [o] - object:0:required totalChannels
		// [o] - object:0:required totalQueues
		// [o] - object:0:required published
		// [o] - object:0:required consumed
		// [o] - object:0:required totalConnections
		// [o] - object:0:required totalMemory
		// [o] - object:0:required currentConnections
		// [o] - object:0:required freeMemory
		// [o] - object:0:required usedMemory
		// [o] - object:0:required totalDirectMemory
		// [o] - object:0:required totalNodes
		// [o] - object:0:required heapPercentage
		// pipeline
		IDataCursor pipelineCursor = pipeline.getCursor();
		String realmProviderURL = getRealmUrl(pipelineCursor);
		IDataUtil.put(pipelineCursor, "realmProviderURL", realmProviderURL);
		int i=0; int j=0;
		pipelineCursor.destroy();
		// pipeline
		IDataCursor pipelineCursor_1 = pipeline.getCursor();
		ArrayList<String> qList = new ArrayList<>();
		ArrayList<String> cList = new ArrayList<>();
		try {
		    realmNode = initiateRealmNode(realmProviderURL);
		    listChannelsAndQueues(realmNode.getNodes(), qList, cList);
		    mySession = initiateSession(realmProviderURL);
		    
		    List<IData> assetsMetrics = new ArrayList<IData>(qList.size() + cList.size());
		    
		    IData[]	queueMetrics = new IData[qList.size()];
		    for (String queueName : qList) {
		        IData queueMetricsData = IDataFactory.create();
		        assetsMetrics.add(queueMetricsData);
		        collectQueueMetrics(queueName, queueMetricsData, pipeline, j, queueMetrics);
		        j++;
		    }
		    
		    
		    IData[]	channelMetrics = new IData[cList.size()];
		    //IData[]	durableMetrics = new IData[2];
		    for (String channelName : cList) {
		    	collectChannelMetrics(channelName, assetsMetrics, pipeline, i, channelMetrics);
		    	i++;
		    }
		    
		    
		    collectCommonRealmMetrics(realmProviderURL, pipeline, cList.size(), qList.size() );
		    
		    IDataUtil.put(pipelineCursor, "assetMetrics", assetsMetrics.toArray(new IData[assetsMetrics.size()]));
		 
		} catch (Exception e) {
		    System.out.println("Error : " + e.getMessage());
		}
		// --- <<IS-END>> ---
		// --- <<IS-END>> ---

                
	}

	// --- <<IS-START-SHARED>> ---
	
	private static void collectCommonRealmMetrics(String realmProviderURL, IData pipeline, int chanSize, int queSize) {
		// pipeline
		IDataCursor pipelineCursor_1 = pipeline.getCursor();
	
		// commonRealmMetrics
		IData[]	commonRealmMetrics = new IData[1];
		commonRealmMetrics[0] = IDataFactory.create();
		IDataCursor commonRealmMetricsCursor = commonRealmMetrics[0].getCursor();
		IDataUtil.put( commonRealmMetricsCursor, "channelSize", chanSize );
		IDataUtil.put( commonRealmMetricsCursor, "queueSize", queSize );
		IDataUtil.put( commonRealmMetricsCursor, "published", realmNode.getTotalPublished() );
		IDataUtil.put( commonRealmMetricsCursor, "consumed", realmNode.getTotalSubscribed() );
		IDataUtil.put( commonRealmMetricsCursor, "totalConnections", realmNode.getTotalConnections() );
		IDataUtil.put( commonRealmMetricsCursor, "currentConnections", realmNode.getCurrentConnections() );
		IDataUtil.put( commonRealmMetricsCursor, "totalMemory", realmNode.getTotalMemory() / (1024 * 1024) );
		IDataUtil.put( commonRealmMetricsCursor, "freeMemory", realmNode.getFreeMemory() / (1024 * 1024) );
		IDataUtil.put( commonRealmMetricsCursor, "usedMemory", (realmNode.getTotalMemory() / (1024 * 1024)) - (realmNode.getFreeMemory() / (1024 * 1024)) );
		IDataUtil.put( commonRealmMetricsCursor, "totalDirectMemory", realmNode.getTotalDirectMemory() / (1024 * 1024) );
		IDataUtil.put( commonRealmMetricsCursor, "totalNodes", realmNode.getTotalNodes() );
		IDataUtil.put( commonRealmMetricsCursor, "heapPercentage", realmNode.getHeapPercentage() );
	
	    nClusterNode clusterNode = realmNode.getCluster();
	    if (clusterNode != null) {
	        Map<String, String> nodeSiteMap = new HashMap<>();
	        Map<String, String> statusMap = new HashMap<>();
	        String clusterInfo = "";
	        String clusterState;
	        List<nClusterStatus> clusterConnectionStatus = clusterNode.getClusterConnectionStatus();
	        for (nClusterStatus status : clusterConnectionStatus) {
	            String mnode = status.getElectedMaster();
	            if (status.getName().equalsIgnoreCase(status.getElectedMaster())) {
	                if (status.getState().equalsIgnoreCase("Master")) {
	                    clusterInfo = clusterInfo + status.getName() + " - " + status.getState() + " | ";
	                    statusMap.put(status.getName(), status.getState());
	                    for (int i = 0; i < status.size(); i++) {
	                        if (!(status.getStatus(i).getName().equalsIgnoreCase(mnode))) {
	                            if (!status.getStatus(i).isOnline()) {
	                                clusterInfo = clusterInfo + status.getStatus(i).getName() + " - Offline" + " | ";
	                                statusMap.put(status.getStatus(i).getName(), "Offline");
	                            } else if (status.getStatus(i).isOnline()) {
	                                clusterInfo = clusterInfo + status.getStatus(i).getName() + " - Slave" + " | ";
	                                statusMap.put(status.getStatus(i).getName(), "Slave");
	                            }
	                        }
	                    }
	                }
	            } else {
	                //statusMap.put(status.getStatus(i).getName(), "Slave");
	            }
	        }
	
	        if (clusterInfo.length() > 1) {
	            clusterState = "true";
	        } else {
	            clusterState = "false";
	        }
	        IDataUtil.put( commonRealmMetricsCursor, "clusterInfo", clusterInfo.toUpperCase() );
	        IDataUtil.put( commonRealmMetricsCursor, "clusterState", clusterState );
	    }
	    commonRealmMetricsCursor.destroy();
	    IDataUtil.put( pipelineCursor_1, "commonRealmMetrics", commonRealmMetrics );
	    pipelineCursor_1.destroy();
	    //realmMetricsCursor.destroy();
	}
	
	private static void collectChannelMetrics(String channelName, List<IData> metricsList, IData pipeline, int i, IData[] channelMetrics) throws Exception {
		// pipeline
		IDataCursor pipelineCursor_1 = pipeline.getCursor();
		
		nChannelAttributes nca = new nChannelAttributes();
	    nca.setName(channelName);
	    nChannel nc = mySession.findChannel(nca);
	    nDurableManager ndm = nc.getDurableManager();
	    nDurable[] nd = ndm.getAll();
	
	    nLeafNode nlf = (nLeafNode) realmNode.findNode(channelName);
	    nca = nlf.getAttributes();
	    
	    for (nDurable nDurable : nd) {
	    	// durableMetrics
	    	IData[]	durableMetrics = new IData[1];
	    	durableMetrics[0] = IDataFactory.create();
	    	IDataCursor durableMetricsCursor = durableMetrics[0].getCursor();
	    	IDataUtil.put( durableMetricsCursor, "name", channelName + " | " + nDurable.getName().replace('#', '_') );
	    	IDataUtil.put( durableMetricsCursor, "queueLength", nDurable.getOutstandingEvents() );
	    	IDataUtil.put( durableMetricsCursor, "numEventsPublished", nDurable.getEID() );
	    	IDataUtil.put( durableMetricsCursor, "type", "Durable" );
	    	IDataUtil.put( durableMetricsCursor, "mode", modeToString(nca.getType()) );
	    	IDataUtil.put( durableMetricsCursor, "totalPublished", "-" );
	    	IDataUtil.put( durableMetricsCursor, "totalConsumed", "-" );
	    	IDataUtil.put( durableMetricsCursor, "lastEventId", nlf.getLastEID() );
	    	IDataUtil.put( durableMetricsCursor, "currentConnections", "-" );
	    	IDataUtil.put( durableMetricsCursor, "totalConnections", "-" );
	    	IDataUtil.put( durableMetricsCursor, "publishRate", "-" );
	    	IDataUtil.put( durableMetricsCursor, "consumedRate", "-" );
	    	IDataUtil.put( durableMetricsCursor, "connectionRate", "-" );
	    	IDataUtil.put( durableMetricsCursor, "fanoutTime", "-" );
	    	IDataUtil.put( durableMetricsCursor, "usedSpaceKb", "-" );
	    	nTopicNode ntn = (nTopicNode) realmNode.findNode(channelName);
	        nDurableNode ndn = ntn.getDurable(nDurable.getName());
	        IDataUtil.put( durableMetricsCursor, "lastEventTime", new Date(ndn.getLastReadTime()) );
	        durableMetricsCursor.destroy();
	        IDataUtil.put( pipelineCursor_1, "durableMetrics", durableMetrics );
	    }
	    
	 /* fullChannelMetrics
	    IData[]	fullChannelMetrics = new IData[1];
	    fullChannelMetrics[0] = IDataFactory.create();
	    IDataCursor fullChannelMetricsCursor = fullChannelMetrics[0].getCursor();*/
	    
	 // channelMetrics
	    
	    //IData[]	channelMetrics = new IData[i+1];
	    channelMetrics[i] = IDataFactory.create();
	    IDataCursor channelMetricsCursor = channelMetrics[i].getCursor();
	    IDataUtil.put( channelMetricsCursor, "channelName", channelName );
	    Object queueLength_1 = new Object();
	    IDataUtil.put( channelMetricsCursor, "queueLength", nlf.getCurrentNumberOfEvents() );
	    IDataUtil.put( channelMetricsCursor, "numEventsPublished", "-" );
	    IDataUtil.put( channelMetricsCursor, "type", "Channel" );
	    IDataUtil.put( channelMetricsCursor, "mode", modeToString(nca.getType()) );
	    Object totalPublished = new Object();
	    IDataUtil.put( channelMetricsCursor, "totalPublished", nlf.getTotalPublished() );
	    Object totalConsumed_1 = new Object();
	    IDataUtil.put( channelMetricsCursor, "totalConsumed", nlf.getTotalConsumed() );
	    Object lastEventId_1 = new Object();
	    IDataUtil.put( channelMetricsCursor, "lastEventId", nlf.getLastEID() );
	    Object currentConnections_2 = new Object();
	    IDataUtil.put( channelMetricsCursor, "currentConnections", nlf.getCurrentNoOfConnections() );
	    Object totalConnections_2 = new Object();
	    IDataUtil.put( channelMetricsCursor, "totalConnections", nlf.getTotalNoOfConnections() );
	    Object publishRate_1 = new Object();
	    IDataUtil.put( channelMetricsCursor, "publishRate", nlf.getPublishRate() );
	    Object consumeRate = new Object();
	    IDataUtil.put( channelMetricsCursor, "consumeRate", nlf.getConsumedRate() );
	    Object connectionRate_1 = new Object();
	    IDataUtil.put( channelMetricsCursor, "connectionRate", nlf.getConnectionRate() );
	    Object fanoutTime_1 = new Object();
	    IDataUtil.put( channelMetricsCursor, "fanoutTime", nlf.getFanoutTime() );
	    Object usedSpaceKb_1 = new Object();
	    IDataUtil.put( channelMetricsCursor, "usedSpaceKb", nlf.getUsedSpace() / 1024.0 );
	    channelMetricsCursor.destroy();
	    //IDataUtil.put( fullChannelMetricsCursor, "channelMetrics", channelMetrics );
	    //fullChannelMetricsCursor.destroy();
	    //IDataUtil.put( pipelineCursor_1, "fullChannelMetrics", fullChannelMetrics );
	    IDataUtil.put( pipelineCursor_1, "channelMetrics", channelMetrics );
	    //channelMetricsCursor.destroy();
	    pipelineCursor_1.destroy();
	}
	
	private static void collectQueueMetrics(String queueName, IData queueMetricsData, IData pipeline, int i, IData[] queueMetrics) throws Exception {
		// pipeline
		IDataCursor pipelineCursor_1 = pipeline.getCursor();
	    nChannelAttributes nca = new nChannelAttributes();
	    nca.setName(queueName);
	    nQueue nq = mySession.findQueue(nca);
	    nQueueDetails nqd = nq.getDetails();
	
	    nLeafNode nlf = (nLeafNode) realmNode.findNode(queueName);
	
	    nca = nlf.getAttributes();
	 // queueMetrics
	    //IData[]	queueMetrics = new IData[1];
	    queueMetrics[i] = IDataFactory.create();
	    IDataCursor queueMetricsCursor = queueMetrics[i].getCursor();
	    IDataUtil.put( queueMetricsCursor, "name", queueName );
	    IDataUtil.put( queueMetricsCursor, "queueLength", nqd.getNoOfEvents() );
	    IDataUtil.put( queueMetricsCursor, "numEventsPublished", nlf.getLastEID() );
	    IDataUtil.put( queueMetricsCursor, "lastEventTime", nqd.getLastEventTime() != 0 ? new Date(nqd.getLastEventTime()) : "-" );
	    IDataUtil.put( queueMetricsCursor, "type", "Queue" );
	    IDataUtil.put( queueMetricsCursor, "mode", modeToString(nca.getType()) );
	    IDataUtil.put( queueMetricsCursor, "totalPubished", nlf.getTotalPublished() );
	    IDataUtil.put( queueMetricsCursor, "totalConsumed", nlf.getTotalConsumed() );
	    IDataUtil.put( queueMetricsCursor, "lastEventId", "-" );
	    IDataUtil.put( queueMetricsCursor, "currentConnections", nlf.getCurrentNoOfConnections() );
	    Object totalConnections_1 = new Object();
	    IDataUtil.put( queueMetricsCursor, "totalConnections", nlf.getTotalNoOfConnections() );
	    IDataUtil.put( queueMetricsCursor, "publishRate", nlf.getPublishRate() );
	    IDataUtil.put( queueMetricsCursor, "consumedRate", nlf.getConsumedRate() );
	    IDataUtil.put( queueMetricsCursor, "connectionRate", nlf.getConnectionRate() );
	    IDataUtil.put( queueMetricsCursor, "fanoutTime", nlf.getFanoutTime() );
	    IDataUtil.put( queueMetricsCursor, "usedSpaceKb", nlf.getUsedSpace() / 1024.0 );
	    queueMetricsCursor.destroy();
	    IDataUtil.put( pipelineCursor_1, "queueMetrics", queueMetrics );
	    pipelineCursor_1.destroy();
	    
	}
	
	private static String getRealmUrl(IDataCursor pipelineCursor) {
	    String realmProviderURL = IDataUtil.getString(pipelineCursor, "realmProviderURL");
	    if (realmProviderURL == null || realmProviderURL.isEmpty()) {
	        String hostName = IDataUtil.getString(pipelineCursor, "hostName");
	        String portNumber = IDataUtil.getString(pipelineCursor, "portNumber");
	        realmProviderURL = "nsp://" + hostName + ":" + portNumber;
	    }
	    
	    return realmProviderURL;
	}
	
	private static String modeToString(int type){
	    if (type == 1) {
	        return  "Reliable";
	    } else if (type == 2) {
	        return  "Persistent";
	    } else if (type == 3) {
	        return  "Mixed";
	    } else if (type == 4) {
	        return  "Simple";
	    } else if (type == 5) {
	        return  "Transient";
	    } else if (type == 7) {
	        return  "Off Heap";
	    } else if (type == 8) {
	        return  "Paged";
	    }
	
	    return "Unknown";
	}
	
	
	public static nRealmNode initiateRealmNode(String realmProviderURL ) throws ServiceException
	{
	if(!mapRealm.containsKey(realmProviderURL))
	{
		String[] RNAME = realmProviderURL.split(",|;");
		try {
			nSessionAttributes nsa=new nSessionAttributes(RNAME);
			realmNode = new nRealmNode(nsa);
			Thread.sleep(2000);
			mapRealm.put(realmProviderURL, realmNode);
		}
		catch(Exception e)
		{
			throw new ServiceException(e.getMessage());
		}
	}
	else
	{
		realmNode = mapRealm.get(realmProviderURL);
		if(!realmNode.isConnected())
		{
			realmNode.close();
			realmNode=null;
			mapRealm.remove(realmProviderURL);
			initiateRealmNode(realmProviderURL);
		}
			
	}
	return realmNode;
	}
	public static nSession initiateSession(String realmProviderURL) throws ServiceException
	{
		if(!mapSession.containsKey(realmProviderURL))
		{
			String[] RNAME = realmProviderURL.split(",|;");
			try {
				nSessionAttributes nsa=new nSessionAttributes(RNAME);
				mySession=nSessionFactory.create(nsa);
				mySession.init();
				
				mapSession.put(realmProviderURL, mySession);
			}
			catch(Exception e)
			{
				throw new ServiceException(e.getMessage());
			}
		}
		else
		{
			mySession = mapSession.get(realmProviderURL);
			if(!mySession.isConnected())
			{
				mySession.close();
				mySession=null;
				mapSession.remove(realmProviderURL);
				initiateSession(realmProviderURL);
			}
		}
	return mySession;
	}
	
	public static void listChannelsAndQueues(Enumeration<?> nodesEnum, ArrayList<String> qList, ArrayList<String> cList) {
	    while (nodesEnum.hasMoreElements()) {
	        Object node = nodesEnum.nextElement();
	
	        if (node instanceof nContainer) {
	            nContainer cont = (nContainer) node;
	            listChannelsAndQueues(cont.getNodes(), qList, cList);
	        } else if (node instanceof nLeafNode) {
	            nLeafNode leafNode = (nLeafNode) node;
	            if (leafNode.isChannel()) {
	                if (!leafNode.getAbsolutePath().equals("/naming/defaultContext")) {
	                    cList.add(leafNode.getAbsolutePath());
	                }
	            }
	            if (leafNode.isQueue()) {
	                qList.add(leafNode.getAbsolutePath());
	            }
	        }
	    }
	}
	
	public static nSession mySession = null;
	public static nRealmNode realmNode = null;
	public static HashMap<String,nSession> mapSession = new HashMap<String,nSession>();
	public static HashMap<String,nRealmNode> mapRealm = new HashMap<String,nRealmNode>();
		
	// --- <<IS-END-SHARED>> ---
}

