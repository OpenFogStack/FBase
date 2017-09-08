package control;

import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.log4j.Logger;

import crypto.CryptoProvider.EncryptionAlgorithm;
import model.config.NodeConfig;
import model.data.NodeID;
import storageconnector.AbstractDBConnector;
import storageconnector.AbstractDBConnector.Connector;

public class Configuration {

	private static Logger logger = Logger.getLogger(Configuration.class.getName());

	private Properties properties;

	// General
	private String machineName = null;
	private String machineIPAddress = null;
	private NodeID nodeID = null;
	private String location = null;
	private String description = null;
	private AbstractDBConnector.Connector databaseConnector = null;

	// Communication
	private Integer restPort = null;
	private Integer messagePort = null;
	private Integer publisherPort = null;

	// Security
	private String privateKey = null;
	private String publicKey = null;

	// Naming Service
	private String namingServiceAddress = null;
	private Integer namingServicePort = null;
	private String namingServicePublicKey = null;

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
			databaseConnector = Connector.valueOf(
					properties.getProperty("databaseConnector", Connector.ON_HEAP.toString()));

			// Communication
			restPort = Integer.parseInt(properties.getProperty("restPort", "-1"));
			messagePort = Integer.parseInt(properties.getProperty("messagePort", "6000"));
			publisherPort = Integer.parseInt(properties.getProperty("publisherPort", "7000"));

			// Security
			privateKey = properties.getProperty("privateKey", "Unknown");
			publicKey = properties.getProperty("publicKey", "Unknown");

			// Naming Service
			namingServiceAddress = properties.getProperty("namingServiceAddress", "Unknown");
			namingServicePort = Integer.parseInt(properties.getProperty("namingServicePort", "-1"));
			namingServicePublicKey = properties.getProperty("namingServicePublicKey", "Unknown");

			// Set IP Address of Machine
			machineIPAddress = InetAddress.getLocalHost().getHostAddress();		
			logger.info("The ip address is " + machineIPAddress);

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

	public NodeConfig buildNodeConfigBasedOnData() {
		NodeConfig config = new NodeConfig();
		config.setNodeID(getNodeID());
		config.setPublicKey(getPublicKey());
		config.setEncryptionAlgorithm(EncryptionAlgorithm.RSA);
		config.setPublisherPort(getPublisherPort());
		config.setMessagePort(getMessagePort());
		config.setRestPort(getRestPort());
		config.setDescription(getDescription());
		config.setLocation(getLocation());
		// add own ip address
		List<String> machines = new ArrayList<>();
		machines.add(getMachineIPAddress());
		config.setMachines(machines);

		return config;
	}

	public String getMachineName() {
		return machineName;
	}

	public String getMachineIPAddress() {
		return machineIPAddress;
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
	
	public Connector getDatabaseConnector() {
		return databaseConnector;
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

	public String getPublicKey() {
		return publicKey;
	}

	public String getNamingServiceAddress() {
		return namingServiceAddress;
	}

	public Integer getNamingServicePort() {
		return namingServicePort;
	}

	public String getNamingServicePublicKey() {
		return namingServicePublicKey;
	}

}
