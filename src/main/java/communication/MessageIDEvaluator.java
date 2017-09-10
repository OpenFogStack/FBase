package communication;

import java.util.ArrayList;
import java.util.List;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;

import control.FBase;
import de.hasenburg.fbase.model.GetMissedMessageResponse;
import exceptions.FBaseCommunicationException;
import exceptions.FBaseStorageConnectorException;
import model.data.MessageID;
import model.data.NodeID;

/**
 * The {@link MessageIDEvaluator} manages all messageIDs received.
 * 
 * Everytime,
 * {@link Subscriber#interpreteReceivedEnvelope(model.messages.Envelope, org.zeromq.ZMQ.Socket)}
 * is called, the subscriber adds the ID of the contained envelope if one is set using
 * {@link #addReceivedMessageID(MessageID)}.
 * 
 * The {@link MessageIDEvaluator} continuously runs through all not evaluated messageIDs. If
 * he finds a gap, he will create a messageSender and ask the related node about the send
 * data.
 * 
 * @author jonathanhasenburg
 *
 */
public class MessageIDEvaluator {

	private static Logger logger = Logger.getLogger(MessageIDEvaluator.class.getName());

	private FBase fBase;
	private ScheduledExecutorService executor = Executors.newScheduledThreadPool(1);

	private TreeMap<NodeID, TreeMap<String, TreeSet<Integer>>> idStorage = new TreeMap<>();

	public MessageIDEvaluator(FBase fBase) {
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
		logger.debug("Running through all messageIDs");
		List<MessageID> missingIDs = getMissingMessageIDs();
		logger.debug("Found " + missingIDs + " missing IDs");
		if (fBase != null) {
			NodeID nodeID = null;
			MessageSender messageSender = null;
			for (MessageID mID : missingIDs) {
				try {
					if (!mID.getNodeID().equals(nodeID)) {
						messageSender.shutdown();
						nodeID = mID.getNodeID();
						logger.debug("Retrieving messages from node " + nodeID);
						messageSender = new MessageSender(
								fBase.configAccessHelper.nodeConfig_get(nodeID), fBase);
					}
					
					GetMissedMessageResponse response = messageSender.sendGetDataRecord(mID);

					if (response.getDataRecord() == null) {
						logger.debug("Data record " + response.getDataIdentifier() + " is deleted");
						fBase.taskmanager.runDeleteDataRecordTask(response.getDataIdentifier(),
								false);
					} else {
						logger.debug("Data record " + response.getDataIdentifier() + " is put");
						fBase.taskmanager.runPutDataRecordTask(response.getDataRecord(), false);
					}
					
					addReceivedMessageID(mID); // to remember that we got it
				} catch (FBaseStorageConnectorException | FBaseCommunicationException
						| NullPointerException e) {
					logger.error("Unable to check on missed messages because cannot get "
							+ "nodeConfig for nodeID " + nodeID);
				}
			}
		} else {
			logger.error("FBase not initialized, so we cannot retrieve missed data");
		}
	};

}
