package control;

import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Properties;

import org.apache.log4j.Logger;

import crypto.CryptoProvider.EncryptionAlgorithm;
import model.data.NodeID;

public class Configuration {

	private static Logger logger = Logger.getLogger(Configuration.class.getName());

	private Properties properties;

	// General
	private String machineName = null;
	private NodeID nodeID = null;
	private String location = null;
	private String description = null;

	// Communication
	private Integer restPort = null;
	private Integer messagePort = null;
	private Integer publisherPort = null;

	// Security
	private String privateKey = null;
	private EncryptionAlgorithm algorithm = null;

	public Configuration(String configName) {
		this.properties = new Properties();
		if (configName == null) {
			configName = "config.properties";
			logger.debug("Reading configuration with default name " + configName);
		}
		InputStream is = Configuration.class.getClassLoader().getResourceAsStream(configName);
		try {
			properties.load(is);
			// General
			machineName = properties.getProperty("machineName");
			nodeID = new NodeID(properties.getProperty("nodeID"));
			location = properties.getProperty("location", "Unknown");
			description = properties.getProperty("description", "Unknown");

			// Communication
			restPort = Integer.parseInt(properties.getProperty("restPort", "-1"));
			messagePort = Integer.parseInt(properties.getProperty("messagePort", "6000"));
			publisherPort = Integer.parseInt(properties.getProperty("publisherPort", "7000"));

			// Security
			privateKey = properties.getProperty("privateKey", "Unknown");
			String alg = properties.getProperty("algorithm");
			if (alg != null)
				algorithm = EncryptionAlgorithm.valueOf(alg);

			checkConsistency();
		} catch (IOException | NumberFormatException e) {
			logger.fatal("Could not read property file, stopping machine");
			e.printStackTrace();
			System.exit(1);
		}
	}

	private void checkConsistency() throws IOException {
		Method[] methods = getClass().getMethods();
		for (int i = 0; i < methods.length; i++) {
			Method m = methods[i];
			if (m.getName().startsWith("get")) {
				if (!m.getName().equals("getAlgorithm")) {
					try {
						Object obj = m.invoke(this);
						if (obj == null) {
							throw new IOException(m.getName() + " must not be null");
						}
					} catch (IllegalAccessException | IllegalArgumentException
							| InvocationTargetException e) {
						e.printStackTrace();
					}
				}
			}

		}

	}

	public String getMachineName() {
		return machineName;
	}

	public NodeID getNodeID() {
		return nodeID;
	}

	public String getLocation() {
		return location;
	}

	public String getDescription() {
		return description;
	}

	public Integer getRestPort() {
		return restPort;
	}

	public Integer getMessagePort() {
		return messagePort;
	}

	public Integer getPublisherPort() {
		return publisherPort;
	}

	public String getPrivateKey() {
		return privateKey;
	}

	public EncryptionAlgorithm getAlgorithm() {
		return algorithm;
	}

}
