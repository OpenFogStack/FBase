package communication;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.log4j.Logger;
import org.zeromq.ZMQ;
import org.zeromq.ZMQ.Socket;

import control.FBase;
import crypto.CryptoProvider.EncryptionAlgorithm;
import de.hasenburg.fbase.model.GetMissedMessageResponse;
import exceptions.FBaseEncryptionException;
import exceptions.FBaseException;
import model.JSONable;
import model.config.NodeConfig;
import model.data.DataIdentifier;
import model.data.DataRecord;
import model.data.MessageID;
import model.messages.Command;
import model.messages.Envelope;
import model.messages.Message;

public class DirectMessageReceiver extends AbstractReceiver {

	private static Logger logger = Logger.getLogger(DirectMessageReceiver.class.getName());

	private FBase fBase = null;

	public DirectMessageReceiver(String address, int port, FBase fBase) {
		super(address, port, ZMQ.REP);
		this.fBase = fBase;
		logger.debug("DirectMessageReceiver ready to receive messages.");
	}

	@Override
	protected void interpreteReceivedEnvelope(Envelope envelope, Socket responseSocket) {
		logger.debug("Received a request with command " + envelope.getMessage().getCommand()
				+ " from node " + envelope.getNodeID());

		// check whether the sender is indeed a node
		if (envelope.getNodeID() == null) {
			logger.debug("Envelope does not contain a nodeID.");
			return;
		}

		Message responseMessage = new Message();

		try {
			// decrypt and verify message
			NodeConfig requestingNode =
					fBase.configAccessHelper.nodeConfig_get(envelope.getNodeID());

			if (requestingNode == null) {
				throw new FBaseException("No config for nodeID " + envelope.getConfigID().getID());
			}

			envelope.getMessage().decryptFields(fBase.configuration.getPrivateKey(),
					EncryptionAlgorithm.RSA);
			if (!envelope.getMessage().verifyMessage(requestingNode.getPublicKey(),
					EncryptionAlgorithm.RSA)) {
				throw new FBaseEncryptionException("The message was not signed correctly");
			}

			// INTERPRET MESSAGE
			// if slow, it might be wise to create another task which executes the processing

			if (Command.GET_DATA_FOR_MESSAGEID.equals(envelope.getMessage().getCommand())) {
				MessageID messageID = new MessageID();
				try {
					messageID.setMessageIDString(envelope.getMessage().getContent());
					DataIdentifier dataID = fBase.connector.messageHistory_get(messageID);
					DataRecord record = fBase.connector.dataRecords_get(dataID);
					responseMessage.setContent(
							JSONable.toJSON(new GetMissedMessageResponse(dataID, record)));
					responseMessage.setTextualInfo("Success");

				} catch (NullPointerException e) {
					responseMessage.setTextualInfo("No data present for messageID");
				} catch (FBaseException e) {
					responseMessage.setTextualInfo("Required messageID not parseable");
				}
			} else if (Command.ANNOUNCE_OWN_NODE_CONFIGURATION_CHANGE.equals(envelope.getMessage().getCommand())) { 
				responseMessage.setContent("true");
				try {
					boolean status = fBase.taskmanager.runAnnounceUpdateOfOwnNodeConfigurationTask().get(5, TimeUnit.SECONDS);
					if (status == false) {
						responseMessage.setContent("Announcing failed, check log of target machine");
					}
				} catch (InterruptedException | ExecutionException | TimeoutException e) {
					responseMessage.setContent(e.getMessage());
				}
				
			} else {
				responseMessage.setTextualInfo(
						"Unknown command " + envelope.getMessage().getCommand().toString());
			}

			// END INTERPRETATION

			// sign and encrypt responseMessage
			logger.debug("Result: " + responseMessage.getTextualInfo());
			if (responseMessage.getContent() != null) {
				// we can only sign if content != null
				responseMessage.signMessage(fBase.configuration.getPrivateKey(),
					EncryptionAlgorithm.RSA);
			}
			responseMessage.encryptFields(requestingNode.getPublicKey(), EncryptionAlgorithm.RSA);
		} catch (FBaseException e) {
			logger.error("Could not process message from " + envelope.getNodeID(), e);
			responseMessage.setTextualInfo("Error, " + e.getMessage());
			logger.debug("Result: " + responseMessage.getTextualInfo());
		}

		responseSocket.send(JSONable.toJSON(responseMessage));

	}

}
