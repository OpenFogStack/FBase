package tasks;

import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;
import org.javatuples.Pair;

import control.FBase;
import exceptions.FBaseCommunicationException;
import exceptions.FBaseException;
import exceptions.FBaseNamingServiceException;
import exceptions.FBaseStorageConnectorException;
import model.JSONable;
import model.config.KeygroupConfig;
import model.config.NodeConfig;
import model.data.KeygroupID;
import model.messages.Command;
import model.messages.Envelope;
import model.messages.Message;
import tasks.TaskManager.TaskName;

class AnnounceUpdateOfOwnNodeConfigurationTask extends Task<Boolean> {

	private static Logger logger =
			Logger.getLogger(AnnounceUpdateOfOwnNodeConfigurationTask.class.getName());

	public AnnounceUpdateOfOwnNodeConfigurationTask(FBase fBase) {
		super(TaskName.ANNOUNCE_UPDATE_OF_OWN_NODE_CONFIGURATION, fBase);
	}

	@Override
	public Boolean executeFunctionality() {

		try {
			// build node config
			NodeConfig nodeConfig = fBase.configuration.buildNodeConfigBasedOnData();
			// add other machines based on heartbeats
			Map<String, Pair<String, Long>> heartbeats = fBase.connector.heartbeats_listAll();
			for (Pair<String, Long> address : heartbeats.values()) {
				if (!address.getValue0().equals(fBase.configuration.getMachineIPAddress())) {
					nodeConfig.addMachine(address.getValue0());
				}
			}

			// naming service
			try {
				// only updates are possible, because a node cannot create itself, only other
				// nodes can
				fBase.namingServiceSender.sendNodeConfigUpdate(nodeConfig);
				logger.info("Updated node configuration at the namingservice");
			} catch (FBaseCommunicationException e) {
				logger.info("Could not update node configuration at the naming service: "
						+ e.getMessage());
			}

			// publish to all keygroups the node is responsible for
			Set<KeygroupID> responsibilities =
					fBase.connector.keyGroupSubscriberMachines_listAll().keySet();
			for (KeygroupID keygroupID : responsibilities) {
				try {
					// get config
					KeygroupConfig config = fBase.configAccessHelper.keygroupConfig_get(keygroupID);
					// publish
					Message m = new Message();
					m.setCommand(Command.UPDATE_FOREIGN_NODE_CONFIG);
					m.setContent(JSONable.toJSON(nodeConfig));
					Envelope e = new Envelope(config.getKeygroupID(), m);
					fBase.publisher.send(e, config.getEncryptionSecret(),
							config.getEncryptionAlgorithm());
				} catch (FBaseException e) {
					logger.error(
							"Could not publish update to recipents of keygroupID " + keygroupID);
				}
			}

		} catch (FBaseStorageConnectorException | FBaseNamingServiceException e) {
			logger.error("Could not announce update to own node configuration", e);
			return false;
		}

		return true;
	}

}
