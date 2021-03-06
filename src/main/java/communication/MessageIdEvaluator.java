package communication;

import java.util.ArrayList;
import java.util.List;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import control.FBase;
import de.hasenburg.fbase.model.GetMissedMessageResponse;
import exceptions.FBaseCommunicationException;
import exceptions.FBaseNamingServiceException;
import exceptions.FBaseStorageConnectorException;
import model.data.MessageID;
import model.data.NodeID;

/**
 * The {@link MessageIdEvaluator} manages all messageIDs received.
 * 
 * Everytime,
 * {@link Subscriber#interpreteReceivedEnvelope(model.messages.Envelope, org.zeromq.ZMQ.Socket)}
 * is called, the subscriber adds the ID of the contained envelope if one is set using
 * {@link #addReceivedMessageID(MessageID)}.
 * 
 * The {@link MessageIdEvaluator} continuously runs through all not evaluated messageIDs. If
 * he finds a gap, he will create a directMessageSender and ask the related node about the send
 * data.
 * 
 * @author jonathanhasenburg
 *
 */
public class MessageIdEvaluator {

	private static Logger logger = Logger.getLogger(MessageIdEvaluator.class.getName());
	
	static {
		logger.setLevel(Level.DEBUG);
	}

	private FBase fBase;
	private ScheduledExecutorService executor = Executors.newScheduledThreadPool(1);

	private TreeMap<NodeID, TreeMap<String, TreeSet<Integer>>> idStorage = new TreeMap<>();

	public MessageIdEvaluator(FBase fBase) {
		this.fBase = fBase;
	}

	public void startup() {
		// run every two seconds
		executor.scheduleWithFixedDelay(evaluateIDs, 0, 2, TimeUnit.SECONDS);
	}

	public void tearDown() {
		executor.shutdownNow();
	}

	public synchronized void addReceivedMessageID(MessageID messageID) {
		logger.debug("Adding " + messageID.getMessageIDString() + " to received IDs");
		getIDSet(getMachineMap(messageID.getNodeID()), messageID.getMachineName())
				.add(messageID.getVersion());
	}

	public synchronized List<MessageID> getMissingMessageIDs() {
		ArrayList<MessageID> missingIDs = new ArrayList<>();
		for (NodeID nodeID : idStorage.keySet()) {
			for (String machineName : getMachineMap(nodeID).keySet()) {
				int i = getIDSet(getMachineMap(nodeID), machineName).first();
				for (Integer version : getIDSet(getMachineMap(nodeID), machineName)) {
					while (version - i > 0) {
						// add MessageIDs
						missingIDs.add(new MessageID(nodeID, machineName, i));
						i++;
					}
					i++;
				}
			}
		}
		return missingIDs;
	}

	private TreeMap<String, TreeSet<Integer>> getMachineMap(NodeID nodeID) {
		TreeMap<String, TreeSet<Integer>> machineMap = idStorage.get(nodeID);
		if (machineMap == null) {
			machineMap = new TreeMap<>();
			idStorage.put(nodeID, machineMap);
		}
		return machineMap;
	}

	private TreeSet<Integer> getIDSet(TreeMap<String, TreeSet<Integer>> machineMap,
			String machineName) {
		TreeSet<Integer> idSet = machineMap.get(machineName);
		if (idSet == null) {
			idSet = new TreeSet<>();
			machineMap.put(machineName, idSet);
		}
		return idSet;
	}

	private Runnable evaluateIDs = () -> {
		List<MessageID> missingIDs = getMissingMessageIDs();
		if (!missingIDs.isEmpty()) {
			logger.debug("Found " + missingIDs.size() + " missing IDs");
		}
		if (fBase != null) {
			NodeID nodeID = null;
			DirectMessageSender directMessageSender = null;
			for (MessageID mID : missingIDs) {
				try {
					if (!mID.getNodeID().equals(nodeID)) {
						if (directMessageSender != null) {
							directMessageSender.shutdown();
						}
						nodeID = mID.getNodeID();
						logger.info("Retrieving missed messages from node " + nodeID);
						directMessageSender = new DirectMessageSender(
								fBase.configAccessHelper.nodeConfig_get(nodeID), fBase);
					}

					GetMissedMessageResponse response = directMessageSender.sendGetDataRecord(mID);

					if (response.getDataIdentifier() == null) {
						logger.debug("Node does not have information about the message anymore");
					} else if (response.getDataRecord() == null) {
						logger.debug("Data record " + response.getDataIdentifier() + " is deleted");
						fBase.taskmanager.runDeleteDataRecordTask(response.getDataIdentifier(),
								false);
					} else {
						logger.debug("Data record " + response.getDataIdentifier() + " is put");
						fBase.taskmanager.runPutDataRecordTask(response.getDataRecord(), false);
					}

					addReceivedMessageID(mID); // to remember that we got it
				} catch (FBaseStorageConnectorException | FBaseCommunicationException
						| NullPointerException | FBaseNamingServiceException e) {
					logger.error("Unable to check on missed messages because cannot get "
							+ "nodeConfig for nodeID " + nodeID, e);
				}
			}
		} else {
			logger.error("FBase not initialized, so we cannot retrieve missed data");
		}
	};

}
