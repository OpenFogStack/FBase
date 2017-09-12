package storageconnector;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;
import org.javatuples.Pair;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.SdkClientException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3ObjectSummary;

import exceptions.FBaseException;
import exceptions.FBaseStorageConnectorException;
import model.JSONable;
import model.config.ClientConfig;
import model.config.KeygroupConfig;
import model.config.NodeConfig;
import model.data.ClientID;
import model.data.DataIdentifier;
import model.data.DataRecord;
import model.data.KeygroupID;
import model.data.MessageID;
import model.data.NodeID;

public class S3DBConnector extends AbstractDBConnector {

	private static final Logger logger = Logger.getLogger(S3DBConnector.class);

	private String bucketPrefix = "de.hasenburg.fbase.s3dbconnector-bucket";

	private enum Suffix {
		DATA_RECORD, KEYGROUP, NODE, CLIENT, SUBSCRIBER_MACHINES, HEARTBEATS, MESSAGE_HISTORY
	}

	private Map<Suffix, String> suffixMap;

	private AmazonS3 s3;

	private NodeID nodeID = null;
	private String machineName = null;

	public S3DBConnector(NodeID nodeID, String machineName) {
		this.nodeID = nodeID;
		this.machineName = machineName;
		suffixMap = new HashMap<>();
		suffixMap.put(Suffix.DATA_RECORD, ".data-records");
		suffixMap.put(Suffix.KEYGROUP, ".keygroup-configs");
		suffixMap.put(Suffix.NODE, ".node-configs");
		suffixMap.put(Suffix.CLIENT, ".client-configs");
		suffixMap.put(Suffix.SUBSCRIBER_MACHINES, ".subscriber-machines");
		suffixMap.put(Suffix.HEARTBEATS, ".heartbeats");
		suffixMap.put(Suffix.MESSAGE_HISTORY, ".message-history");
	}

	public S3DBConnector(NodeID nodeID, String machineName, String bucketName) {
		this(nodeID, machineName);
		this.bucketPrefix = bucketName;
	}
	
	/**
	 * Mainly used for tests.
	 * 
	 * @param nodeID
	 * @param machineName
	 */
	public void setNodeIDAndMachineName(NodeID nodeID, String machineName) {
		this.nodeID = nodeID;
		this.machineName = machineName;
	}

	@Override
	public void dbConnection_initiate() throws FBaseStorageConnectorException {
		try {
			s3 = AmazonS3ClientBuilder.defaultClient();
		} catch (SdkClientException e) {
			logger.fatal("Could not initiate S3 Client, do you have credentials set?", e);
			System.exit(1);
		}
		for (String suffix : suffixMap.values()) {
			String bucketName = bucketPrefix + suffix;
			if (s3.doesBucketExist(bucketName)) {
				logger.debug("Bucket " + bucketName + " already exists.");
			} else {
				try {
					s3.createBucket(bucketName);
				} catch (AmazonS3Exception e) {
					System.err.println(e.getErrorMessage());
				}
			}
		}
	}

	public void deleteBuckets() throws FBaseStorageConnectorException {
		logger.info("Deleting buckets " + bucketPrefix);
		for (String suffix : suffixMap.values()) {
			String bucketName = bucketPrefix + suffix;
			try {
				ObjectListing objects = s3.listObjects(bucketName);
				while (true) {
					for (Iterator<?> iterator = objects.getObjectSummaries().iterator(); iterator
							.hasNext();) {
						S3ObjectSummary summary = (S3ObjectSummary) iterator.next();
						s3.deleteObject(bucketName, summary.getKey());
					}

					// check if truncated
					if (objects.isTruncated()) {
						objects = s3.listNextBatchOfObjects(objects);
					} else {
						break;
					}
				}
				s3.deleteBucket(bucketName);
			} catch (AmazonServiceException e) {
				throw new FBaseStorageConnectorException(e);
			}
		}
	}

	@Override
	public void dbConnection_close() {
		// nothing needs to be done here
	}

	private String getDataRecordBucketName() {
		return bucketPrefix + suffixMap.get(Suffix.DATA_RECORD);
	}

	private String getDataRecordPath(DataIdentifier identifier) {
		return identifier.toString();
	}

	private String getDataRecordPath(DataRecord record) {
		return getDataRecordPath(record.getDataIdentifier());
	}

	@Override
	public void dataRecords_put(DataRecord record) throws FBaseStorageConnectorException {
		if (!s3.doesObjectExist(getDataRecordBucketName(),
				record.getDataIdentifier().getKeygroupID().toString())) {
			// even though not really necessary, this exception ensures same behavior of
			// connectors
			throw new FBaseStorageConnectorException("Keygroup does not exist.");
		}
		try {
			s3.putObject(getDataRecordBucketName(), getDataRecordPath(record),
					JSONable.toJSON(record));
		} catch (AmazonServiceException e) {
			throw new FBaseStorageConnectorException(e);
		}
	}

	@Override
	public DataRecord dataRecords_get(DataIdentifier dataIdentifier)
			throws FBaseStorageConnectorException {
		try {
			DataRecord record =
					JSONable.fromJSON(
							s3.getObject(getDataRecordBucketName(),
									getDataRecordPath(dataIdentifier)).getObjectContent(),
							DataRecord.class);
			return record;
		} catch (AmazonServiceException e) {
			if (404 == e.getStatusCode()) {
				return null;
			}
			throw new FBaseStorageConnectorException(e);
		}
	}

	@Override
	public boolean dataRecords_delete(DataIdentifier dataIdentifier)
			throws FBaseStorageConnectorException {
		try {
			s3.deleteObject(getDataRecordBucketName(), getDataRecordPath(dataIdentifier));
			return true;
		} catch (AmazonServiceException e) {
			throw new FBaseStorageConnectorException(e);
		}
	}

	@Override
	public Set<DataIdentifier> dataRecords_list(KeygroupID keygroupID)
			throws FBaseStorageConnectorException {
		try {
			Set<DataIdentifier> identifiers = new HashSet<>();
			ObjectListing ol =
					s3.listObjects(getDataRecordBucketName(), keygroupID.toString() + "/");
			while (true) {
				List<S3ObjectSummary> objects = ol.getObjectSummaries();
				for (S3ObjectSummary os : objects) {
					identifiers.add(DataIdentifier.createFromString(os.getKey()));
				}
				// check if truncated
				if (ol.isTruncated()) {
					ol = s3.listNextBatchOfObjects(ol);
				} else {
					break;
				}
			}

			return identifiers;
		} catch (AmazonServiceException e) {
			throw new FBaseStorageConnectorException(e);
		}
	}

	@Override
	public boolean keygroup_create(KeygroupID keygroupID) throws FBaseStorageConnectorException {
		try {
			s3.putObject(getDataRecordBucketName(), keygroupID.toString(), "");
			return true;
		} catch (AmazonServiceException e) {
			throw new FBaseStorageConnectorException(e);
		}
	}

	@Override
	public boolean keygroup_delete(KeygroupID keygroupID) throws FBaseStorageConnectorException {
		try {
			s3.deleteObject(getDataRecordBucketName(), keygroupID.toString());
			return true;
		} catch (AmazonServiceException e) {
			throw new FBaseStorageConnectorException(e);
		}
	}

	private String getKeygroupConfigBucketName() {
		return bucketPrefix + suffixMap.get(Suffix.KEYGROUP);
	}

	private String getKeygroupConfigPath(KeygroupID keygroupID) {
		return keygroupID.toString();
	}

	@Override
	public void keygroupConfig_put(KeygroupID keygroupID, KeygroupConfig config)
			throws FBaseStorageConnectorException {
		try {
			s3.putObject(getKeygroupConfigBucketName(), getKeygroupConfigPath(keygroupID),
					JSONable.toJSON(config));
		} catch (AmazonServiceException e) {
			throw new FBaseStorageConnectorException(e);
		}
	}

	@Override
	public KeygroupConfig keygroupConfig_get(KeygroupID keygroupID)
			throws FBaseStorageConnectorException {
		try {
			KeygroupConfig config =
					JSONable.fromJSON(
							s3.getObject(getKeygroupConfigBucketName(),
									getKeygroupConfigPath(keygroupID)).getObjectContent(),
							KeygroupConfig.class);
			return config;
		} catch (AmazonServiceException e) {
			if (404 == e.getStatusCode()) {
				return null;
			}
			throw new FBaseStorageConnectorException(e);
		}
	}

	@Override
	public List<KeygroupID> keygroupConfig_list() throws FBaseStorageConnectorException {
		try {
			List<KeygroupID> identifiers = new ArrayList<>();
			ObjectListing ol = s3.listObjects(getKeygroupConfigBucketName());
			while (true) {
				List<S3ObjectSummary> objects = ol.getObjectSummaries();
				for (S3ObjectSummary os : objects) {
					identifiers.add(KeygroupID.createFromString(os.getKey()));
				}
				// check if truncated
				if (ol.isTruncated()) {
					ol = s3.listNextBatchOfObjects(ol);
				} else {
					break;
				}
			}
			return identifiers;
		} catch (AmazonServiceException e) {
			throw new FBaseStorageConnectorException(e);
		}
	}

	private String getNodeConfigBucketName() {
		return bucketPrefix + suffixMap.get(Suffix.NODE);
	}

	private String getNodeConfigPath(NodeID nodeID) {
		return nodeID.toString();
	}

	@Override
	public void nodeConfig_put(NodeID nodeID, NodeConfig config)
			throws FBaseStorageConnectorException {
		try {
			s3.putObject(getNodeConfigBucketName(), getNodeConfigPath(nodeID),
					JSONable.toJSON(config));
		} catch (AmazonServiceException e) {
			throw new FBaseStorageConnectorException(e);
		}
	}

	@Override
	public NodeConfig nodeConfig_get(NodeID nodeID) throws FBaseStorageConnectorException {
		try {
			NodeConfig config = JSONable
					.fromJSON(s3.getObject(getNodeConfigBucketName(), getNodeConfigPath(nodeID))
							.getObjectContent(), NodeConfig.class);
			return config;
		} catch (AmazonServiceException e) {
			if (404 == e.getStatusCode()) {
				return null;
			}
			throw new FBaseStorageConnectorException(e);
		}
	}

	@Override
	public List<NodeID> nodeConfig_list() throws FBaseStorageConnectorException {
		try {
			List<NodeID> identifiers = new ArrayList<>();
			ObjectListing ol = s3.listObjects(getNodeConfigBucketName());
			while (true) {
				List<S3ObjectSummary> objects = ol.getObjectSummaries();
				for (S3ObjectSummary os : objects) {
					identifiers.add(new NodeID(os.getKey()));
				}
				// check if truncated
				if (ol.isTruncated()) {
					ol = s3.listNextBatchOfObjects(ol);
				} else {
					break;
				}
			}
			return identifiers;
		} catch (AmazonServiceException e) {
			throw new FBaseStorageConnectorException(e);
		}
	}

	private String getClientConfigBucketName() {
		return bucketPrefix + suffixMap.get(Suffix.CLIENT);
	}

	private String getClientConfigPath(ClientID clientID) {
		return clientID.toString();
	}

	@Override
	public void clientConfig_put(ClientID clientID, ClientConfig config)
			throws FBaseStorageConnectorException {
		try {
			s3.putObject(getClientConfigBucketName(), getClientConfigPath(clientID),
					JSONable.toJSON(config));
		} catch (AmazonServiceException e) {
			throw new FBaseStorageConnectorException(e);
		}
	}

	@Override
	public ClientConfig clientConfig_get(ClientID clientID) throws FBaseStorageConnectorException {
		try {
			ClientConfig config = JSONable.fromJSON(
					s3.getObject(getClientConfigBucketName(), getClientConfigPath(clientID))
							.getObjectContent(),
					ClientConfig.class);
			return config;
		} catch (AmazonServiceException e) {
			if (404 == e.getStatusCode()) {
				return null;
			}
			throw new FBaseStorageConnectorException(e);
		}
	}

	@Override
	public List<ClientID> clientConfig_list() throws FBaseStorageConnectorException {
		try {
			List<ClientID> identifiers = new ArrayList<>();
			ObjectListing ol = s3.listObjects(getClientConfigBucketName());
			while (true) {
				List<S3ObjectSummary> objects = ol.getObjectSummaries();
				for (S3ObjectSummary os : objects) {
					identifiers.add(new ClientID(os.getKey()));
				}
				// check if truncated
				if (ol.isTruncated()) {
					ol = s3.listNextBatchOfObjects(ol);
				} else {
					break;
				}
			}
			return identifiers;
		} catch (AmazonServiceException e) {
			throw new FBaseStorageConnectorException(e);
		}
	}

	private String getSubscriberMachinesBucketName() {
		return bucketPrefix + suffixMap.get(Suffix.SUBSCRIBER_MACHINES);
	}

	private String getSubscriberMachinesPath(KeygroupID keygroupID, String machine) {
		return keygroupID.toString() + "/" + machine;
	}

	private int getVersionForSubscriberMachinePath(String path)
			throws FBaseStorageConnectorException {
		int version = 0;
		try {
			Map<String, String> metaDataMap =
					s3.getObjectMetadata(getSubscriberMachinesBucketName(), path).getUserMetadata();
			String versionString = metaDataMap.get("version");
			try {
				version = Integer.parseInt(versionString);
			} catch (NumberFormatException | NullPointerException e) {
				logger.error("Version stored on S3 can't be parsed: " + versionString, e);
			}
		} catch (AmazonServiceException e) {
			if (404 != e.getStatusCode()) {
				throw new FBaseStorageConnectorException(e);
			}
			// if we get here, there simply was not a version set, so everything is ok
		}
		return version;
	}

	@Override
	public Integer keyGroupSubscriberMachines_put(KeygroupID keygroupID, String machine)
			throws FBaseStorageConnectorException {
		int version =
				getVersionForSubscriberMachinePath(getSubscriberMachinesPath(keygroupID, machine));

		version++;
		try {
			// create hashmap with version
			HashMap<String, String> metadataMap = new HashMap<>();
			metadataMap.put("version", "" + version);

			// create metadata
			byte[] contentBytes = "".getBytes(StandardCharsets.UTF_8);
			InputStream contentStream = new ByteArrayInputStream(contentBytes);
			ObjectMetadata metadata = new ObjectMetadata();
			metadata.setUserMetadata(metadataMap);

			// put object
			PutObjectRequest putObjectRequest = new PutObjectRequest(
					getSubscriberMachinesBucketName(),
					getSubscriberMachinesPath(keygroupID, machine), contentStream, metadata);
			s3.putObject(putObjectRequest);
		} catch (AmazonServiceException e) {
			throw new FBaseStorageConnectorException(e);
		}
		return version;
	}

	@Override
	public Map<KeygroupID, Pair<String, Integer>> keyGroupSubscriberMachines_listAll()
			throws FBaseStorageConnectorException {
		try {
			Map<KeygroupID, Pair<String, Integer>> map = new HashMap<>();
			ObjectListing ol = s3.listObjects(getSubscriberMachinesBucketName());
			while (true) {
				List<S3ObjectSummary> objects = ol.getObjectSummaries();
				for (S3ObjectSummary os : objects) {
					String[] dataA = os.getKey().split("/");
					if (dataA.length == 4) {
						// put object in map
						KeygroupID keygroupID = new KeygroupID(dataA[0], dataA[1], dataA[2]);
						String machine = dataA[3];
						Integer version = getVersionForSubscriberMachinePath(os.getKey());
						map.put(keygroupID, new Pair<String, Integer>(machine, version));
					} else {
						logger.warn("Could not read " + os.getKey());
					}
				}
				// check if truncated
				if (ol.isTruncated()) {
					ol = s3.listNextBatchOfObjects(ol);
				} else {
					break;
				}
			}
			return map;
		} catch (AmazonServiceException e) {
			throw new FBaseStorageConnectorException(e);
		}
	}

	@Override
	public void keyGroupSubscriberMachines_remove(KeygroupID keygroupID)
			throws FBaseStorageConnectorException {
		// we need to find the object based on the keygroupID
		ObjectListing ol = s3.listObjects(getSubscriberMachinesBucketName(),
				getSubscriberMachinesPath(keygroupID, ""));
		while (true) {
			List<S3ObjectSummary> objects = ol.getObjectSummaries();
			for (S3ObjectSummary os : objects) {
				try {
					s3.deleteObject(getSubscriberMachinesBucketName(), os.getKey());
				} catch (AmazonServiceException e) {
					throw new FBaseStorageConnectorException(e);
				}
			}
			// check if truncated
			if (ol.isTruncated()) {
				ol = s3.listNextBatchOfObjects(ol);
			} else {
				break;
			}
		}
	}

	private String getHeartbeatBucketName() {
		return bucketPrefix + suffixMap.get(Suffix.HEARTBEATS);
	}

	private String getHeartbeatsPath(String machine) {
		return machine;
	}

	@Override
	public void heartbeats_update(String machine) throws FBaseStorageConnectorException {
		try {
			s3.putObject(getHeartbeatBucketName(), getHeartbeatsPath(machine),
					Long.toString(System.currentTimeMillis()));
		} catch (AmazonServiceException e) {
			throw new FBaseStorageConnectorException(e);
		}
	}

	@Override
	public Map<String, Long> heartbeats_getAll() throws FBaseStorageConnectorException {
		try {
			Map<String, Long> heartbeats = new HashMap<>();
			ObjectListing ol = s3.listObjects(getHeartbeatBucketName());
			while (true) {
				List<S3ObjectSummary> objects = ol.getObjectSummaries();
				for (S3ObjectSummary os : objects) {
					BufferedReader reader = new BufferedReader(new InputStreamReader(s3
							.getObject(getHeartbeatBucketName(), os.getKey()).getObjectContent()));
					try {
						Long time = Long.parseLong(reader.readLine());
						heartbeats.put(os.getKey(), time);
					} catch (NumberFormatException | IOException e) {
						logger.error("Cannot parse time from " + os.getKey(), e);
					}
				}
				// check if truncated
				if (ol.isTruncated()) {
					ol = s3.listNextBatchOfObjects(ol);
				} else {
					break;
				}
			}
			return heartbeats;
		} catch (AmazonServiceException e) {
			throw new FBaseStorageConnectorException(e);
		}
	}

	private String getMessageBucketName() {
		return bucketPrefix + suffixMap.get(Suffix.MESSAGE_HISTORY);
	}

	private String getMessageIDPath(MessageID messageID) {
		return messageID.getMessageIDString();
	}

	@Override
	public MessageID messageHistory_getNextMessageID() throws FBaseStorageConnectorException {

		// so we can test it without initializing FBase

		try {

			ObjectListing ol = s3.listObjects(getMessageBucketName(), nodeID + "/" + machineName);

			// no data
			List<S3ObjectSummary> objects = ol.getObjectSummaries();
			if (objects.isEmpty()) {
				return new MessageID(nodeID, machineName, 1);
			}

			// get last batch
			while (ol.isTruncated()) {
				ol = s3.listNextBatchOfObjects(ol);
			}

			// get last item
			objects = ol.getObjectSummaries();
			MessageID messageID = new MessageID();
			try {
				messageID.setMessageIDString(objects.get(objects.size() - 1).getKey());
			} catch (FBaseException e) {
				throw new FBaseStorageConnectorException(
						"Cannot get version from " + objects.get(objects.size() - 1).getKey());
			}

			int nextVersion = messageID.getVersion() + 1;
			return new MessageID(nodeID, machineName, nextVersion);

		} catch (AmazonServiceException e) {
			throw new FBaseStorageConnectorException(e);
		}
	}

	@Override
	public void messageHistory_put(MessageID messageID, DataIdentifier relatedData)
			throws FBaseStorageConnectorException {
		try {
			s3.putObject(getMessageBucketName(), getMessageIDPath(messageID),
					JSONable.toJSON(relatedData));
		} catch (AmazonServiceException e) {
			throw new FBaseStorageConnectorException(e);
		}
	}

	@Override
	public DataIdentifier messageHistory_get(MessageID messageID)
			throws FBaseStorageConnectorException {
		try {
			DataIdentifier identifier = JSONable
					.fromJSON(s3.getObject(getMessageBucketName(), getMessageIDPath(messageID))
							.getObjectContent(), DataIdentifier.class);
			return identifier;
		} catch (AmazonServiceException e) {
			if (404 == e.getStatusCode()) {
				return null;
			}
			throw new FBaseStorageConnectorException(e);
		}
	}

}
