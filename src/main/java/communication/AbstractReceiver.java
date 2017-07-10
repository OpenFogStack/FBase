package communication;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.log4j.Logger;
import org.zeromq.ZMQ;
import org.zeromq.ZMQException;

import crypto.CryptoProvider;
import crypto.CryptoProvider.EncryptionAlgorithm;
import model.data.KeygroupID;
import model.message.Envelope;
import model.message.Message;

/**
 * Abstract class for {@link Subscriber} and {@link GetRequestHandler}
 * 
 * @author jonathanhasenburg
 *
 */
public abstract class AbstractReceiver {

	private static Logger logger = Logger.getLogger(AbstractReceiver.class.getName());

	/**
	 * The address used for the reception of messages.
	 */
	private String address = "";

	/**
	 * The port used for the reception of messages.
	 */
	private int port = -1;

	/**
	 * The secret used for the encryption of messages
	 */
	protected String secret;

	/**
	 * The algorithm used for the encryption of messages
	 */
	protected EncryptionAlgorithm algorithm;

	/**
	 * The number of messages that have been received until now
	 */
	private int numberOfReceivedMessages = 0;

	/**
	 * The executor that is used to execute the runnable which is used for reception.
	 */
	private ExecutorService executor = null;

	/**
	 * Future of the runnable which is used for reception.
	 */
	private Future<?> runnableFuture = null;

	/**
	 * The receiver type. Possible values are ZMQ.SUB and ZMQ.REP.
	 */
	private int receiverType;
	
	/**
	 * If the used socket is a subscriber, the keygroup filter might be used to only receive messages with a
	 * specific topic/keygroup.
	 */
	protected KeygroupID keygroupIDFilter = null;

	private ZMQ.Context context = null;
	private ZMQ.Socket socket = null;

	public AbstractReceiver(String address, int port, String secret, EncryptionAlgorithm algorithm, int receiverType) {
		this.address = address;
		this.port = port;
		this.secret = secret;
		this.algorithm = algorithm;
		if (receiverType != ZMQ.SUB && receiverType != ZMQ.REP) {
			throw new IllegalArgumentException("Receiver type " + receiverType + " is not valid.");
		}
		this.receiverType = receiverType;
		this.executor = Executors.newSingleThreadExecutor();
	}

	/**
	 * @return {@link #address}
	 */
	public String getAddress() {
		return address;
	}

	/**
	 * @return {@link #port}
	 */
	public int getPort() {
		return port;
	}

	/**
	 * @return {@link #numberOfReceivedMessages}
	 */
	public int getNumberOfReceivedMessages() {
		return numberOfReceivedMessages;
	}

	/**
	 * Increments {@link #numberOfReceivedMessages} by one.
	 */
	protected void incrementNumberOfReceivedMessages() {
		numberOfReceivedMessages++;
	}
	
	/**
	 * @return {@link #keygroupIDFilter}
	 */
	public KeygroupID getkeygroupIDFilter() {
		return keygroupIDFilter;
	}
	
	/**
	 * Starts reception and interpretation of incoming envelopes..
	 * 
	 * @return <code>true</code> if reception was not running before and operation successful, 
	 * <code>false</code> otherwise
	 */
	public Future<?> startReception() {
		if (runnableFuture != null) {
			if (!runnableFuture.isDone()) {
				logger.error("Did not start reception because already running.");
				return null;
			}
		}
		runnableFuture = executor.submit(new Runnable() {
			
			private boolean init() {
				if (receiverType == ZMQ.SUB) {
					context = ZMQ.context(1);
					socket = context.socket(ZMQ.SUB);
					socket.connect(getAddress() + ":" + getPort());
					if (keygroupIDFilter != null) {
						socket.subscribe(keygroupIDFilter.toString().getBytes());
					} else {
						socket.subscribe("".getBytes());
					}
					return true;
				} else if (receiverType == ZMQ.REP) {
					context = ZMQ.context(1);
					socket = context.socket(ZMQ.REP);
					socket.bind(getAddress() + ":" + getPort());
					return true;
				} else {
					logger.error(receiverType + " is not a valid socket type");
					return false;
				}
			}
			
			@Override
			public void run() {
				
				if (!init()) return;
				
				while (true) {
					Envelope envelope = new Envelope(null, null);
					boolean envelopeFine = true;
					
					try {
						boolean more = true;
						while (more) {
							String s = socket.recvStr();
							if (envelope.getKeygroupID() == null) {
								envelope.setKeygroupID(KeygroupID.createFromString(s));
								logger.debug("Received keygroupID: " + envelope.getKeygroupID());
							} else if (envelope.getMessage() == null) {
								envelope.setMessage(Message.fromJSON(
										CryptoProvider.decrypt(s, secret, algorithm), Message.class));
								logger.debug("Received content: " + envelope.getMessage().getContent());
							} else {
								logger.error("Received more mulitpart messages than expected, dismissing: " + s);
								envelopeFine = false;
							}
							more = socket.hasReceiveMore();
						}
					} catch (ZMQException e) {
						logger.debug("Context was terminated, thread is dying.");
						break;
					}
				
					if (envelope.getKeygroupID() == null || envelope.getMessage() == null) {
						logger.error("Envelope incomplete");
					} else if (!envelopeFine){
						logger.error("Envelope broken");
					} else {
						incrementNumberOfReceivedMessages();
						logger.debug("Received complete message, keygroupID: " + envelope.getKeygroupID());
						interpetReceivedEnvelope(envelope, socket);
					}
				}
			}	

		});
		try {
			Thread.sleep(500); // so that reception started before returning
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		logger.info("Started reception of incoming envelopes.");
		return runnableFuture;
	}

	/**
	 * Stops the reception of envelopes immediately.
	 */
	public void stopReception() {
		if (runnableFuture != null) {
			runnableFuture = null;
			socket.close();
			context.term();
		}	
		logger.info("Reception of incoming envelopes is stopped.");
	}

	/**
	 * Checks whether reception of envelopes is currently running.
	 * 
	 * @return <code>true</code> if currently receiving, false otherwise
	 */
	public boolean isReceiving() {
		if (runnableFuture != null) {
			return !runnableFuture.isDone();
		}
		return false;
	}

	protected abstract void interpetReceivedEnvelope(Envelope envelope, ZMQ.Socket responseSocket);

}
