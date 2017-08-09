package communication;

import org.apache.log4j.Logger;
import org.zeromq.ZMQ;

import control.FBase;
import crypto.CryptoProvider.EncryptionAlgorithm;
import exceptions.FBaseNamingServiceException;
import model.JSONable;
import model.config.NodeConfig;
import model.messages.Command;
import model.messages.Envelope;
import model.messages.Message;

/**
 * Sends requests to designated receivers.
 * 
 * @author jonathanhasenburg
 *
 */
public class NamingServiceSender extends AbstractSender {

	private static Logger logger = Logger.getLogger(NamingServiceSender.class.getName());

	private FBase fBase;

	/**
	 * Initializes the NamingServiceSender, it then can be used without further modifications.
	 */
	public NamingServiceSender(String address, int port, FBase fBase) {
		super(address, port, ZMQ.REQ);
		this.fBase = fBase;
	}

	/**
	 * Sends an envelope to the specified address.
	 * 
	 * @param envelope
	 * @return the response
	 * @throws FBaseNamingServiceException
	 */
	@Override
	public String send(Envelope envelope, String secret, EncryptionAlgorithm algorithm)
			throws FBaseNamingServiceException {
		sender.sendMore(envelope.getKeygroupID().getID());
		sender.send(JSONable.toJSON(envelope.getMessage()));

		ZMQ.Poller poller = context.poller();
		poller.register(sender, ZMQ.Poller.POLLIN);
		long rc = poller.poll(3000);
		if (rc == -1)
			throw new FBaseNamingServiceException(FBaseNamingServiceException.NOT_REACHABLE);

		if (poller.pollin(0)) {
			// We got a reply from the server, must match sequence
			String reply = sender.recvStr();
			return reply;
		} else {
			logger.warn("Did not get a response from naming service, recreating socket");
			sender.setLinger(0); // drop pending messages when closed
			sender.close();
			poller.unregister(sender);
			sender = context.socket(ZMQ.REQ);
			sender.connect(getAddress() + ":" + getPort());
			throw new FBaseNamingServiceException(FBaseNamingServiceException.NOT_REACHABLE);
		}
	}

	public String sendNodeConfigCreate(NodeConfig nodeConfig) {
		Message m = new Message();
		m.setCommand(Command.NODE_CONFIG_CREATE);
		m.setContent(JSONable.toJSON(nodeConfig));
		m.encryptFields(fBase.configuration.getPrivateKey(),
				EncryptionAlgorithm.RSA_PRIVATE_ENCRYPT);
		// TODO encrypt with NS public key
		Envelope e = new Envelope(fBase.configuration.getNodeID(), m);

		try {
			String answer = send(e, null, null);
			// TODO process response
			return answer;
		} catch (FBaseNamingServiceException e1) {
			logger.error(e1.getMessage());
		}
		
		return null;

	}

}
