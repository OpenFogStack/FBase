package storageconnector;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
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

import control.FBase;
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

	private String bucketName = "de.hasenburg.fbase.s3dbconnector-bucket";
	private AmazonS3 s3;
	
	@SuppressWarnings("unused")
	private FBase fBase = null;

	public S3DBConnector(FBase fBase) {
		this.fBase = fBase;
	}

	public S3DBConnector(FBase fBase, String bucketName) {
		this.fBase = fBase;
		this.bucketName = bucketName;
	}

	@Override
	public void dbConnection_initiate() throws FBaseStorageConnectorException {
		try {
			s3 = AmazonS3ClientBuilder.defaultClient();
		} catch (SdkClientException e) {
			logger.fatal("Could not initiate S3 Client, do you have credentials set?", e);
			System.exit(1);
		}
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

	public void deleteBucket() throws FBaseStorageConnectorException {
		logger.info("Deleting bucket " + bucketName);
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

	@Override
	public void dbConnection_close() {
		// nothing needs to be done here
	}

	@Override
	public void dataRecords_put(DataRecord record) throws FBaseStorageConnectorException {
		if (!s3.doesObjectExist(bucketName,
				record.getDataIdentifier().getKeygroupID().toString())) {
			throw new FBaseStorageConnectorException("Keygroup does not exist.");
		}
		try {
			s3.putObject(bucketName, record.getDataIdentifier().toString(),
					JSONable.toJSON(record));
		} catch (AmazonServiceException e) {
			throw new FBaseStorageConnectorException(e);
		}
	}

	@Override
	public DataRecord dataRecords_get(DataIdentifier dataIdentifier)
			throws FBaseStorageConnectorException {
		try {
			DataRecord record = JSONable.fromJSON(
					s3.getObject(bucketName, dataIdentifier.toString()).getObjectContent(),
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
			s3.deleteObject(bucketName, dataIdentifier.toString());
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
			ObjectListing ol = s3.listObjects(bucketName);
			List<S3ObjectSummary> objects = ol.getObjectSummaries();
			for (S3ObjectSummary os : objects) {
				if (os.getKey().startsWith(keygroupID.toString() + "/")) {
					identifiers.add(DataIdentifier.createFromString(os.getKey()));
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
			s3.putObject(bucketName, keygroupID.toString(), "");
			return true;
		} catch (AmazonServiceException e) {
			throw new FBaseStorageConnectorException(e);
		}
	}

	@Override
	public boolean keygroup_delete(KeygroupID keygroupID) throws FBaseStorageConnectorException {
		try {
			s3.deleteObject(bucketName, keygroupID.toString());
			return true;
		} catch (AmazonServiceException e) {
			throw new FBaseStorageConnectorException(e);
		}
	}

	private String getKeygroupConfigPath(KeygroupID keygroupID) {
		return "KeygroupConfigs/" + keygroupID.toString();
	}

	@Override
	public void keygroupConfig_put(KeygroupID keygroupID, KeygroupConfig config)
			throws FBaseStorageConnectorException {
		try {
			s3.putObject(bucketName, getKeygroupConfigPath(keygroupID), JSONable.toJSON(config));
		} catch (AmazonServiceException e) {
			throw new FBaseStorageConnectorException(e);
		}
	}

	@Override
	public KeygroupConfig keygroupConfig_get(KeygroupID keygroupID)
			throws FBaseStorageConnectorException {
		try {
			KeygroupConfig config = JSONable.fromJSON(
					s3.getObject(bucketName, getKeygroupConfigPath(keygroupID)).getObjectContent(),
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
		// TODO 1: IMPLEMENT
		throw new RuntimeException("NOT YET IMPLEMENTED!");
	}

	private String getNodeConfigPath(NodeID nodeID) {
		return "NodeConfigs/" + nodeID.toString();
	}

	@Override
	public void nodeConfig_put(NodeID nodeID, NodeConfig config)
			throws FBaseStorageConnectorException {
		try {
			s3.putObject(bucketName, getNodeConfigPath(nodeID), JSONable.toJSON(config));
		} catch (AmazonServiceException e) {
			throw new FBaseStorageConnectorException(e);
		}
	}

	@Override
	public NodeConfig nodeConfig_get(NodeID nodeID) throws FBaseStorageConnectorException {
		try {
			NodeConfig config = JSONable.fromJSON(
					s3.getObject(bucketName, getNodeConfigPath(nodeID)).getObjectContent(),
					NodeConfig.class);
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
		// TODO 1: IMPLEMENT
		throw new RuntimeException("NOT YET IMPLEMENTED!");
	}

	private String getClientConfigPath(ClientID clientID) {
		return "ClientConfigs/" + clientID.toString();
	}

	@Override
	public void clientConfig_put(ClientID clientID, ClientConfig config)
			throws FBaseStorageConnectorException {
		try {
			s3.putObject(bucketName, getClientConfigPath(clientID), JSONable.toJSON(config));
		} catch (AmazonServiceException e) {
			throw new FBaseStorageConnectorException(e);
		}
	}

	@Override
	public ClientConfig clientConfig_get(ClientID clientID) throws FBaseStorageConnectorException {
		try {
			ClientConfig config = JSONable.fromJSON(
					s3.getObject(bucketName, getClientConfigPath(clientID)).getObjectContent(),
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
		// TODO 1: IMPLEMENT
		throw new RuntimeException("NOT YET IMPLEMENTED!");
	}

	private String getSubscriberMachinesPath(KeygroupID keygroupID, String machine) {
		return "SubscriberMachines/" + keygroupID.toString() + "/" + machine;
	}

	private int getVersionForSubscriberMachinePath(String path)
			throws FBaseStorageConnectorException {
		int version = 0;
		try {
			Map<String, String> metaDataMap =
					s3.getObjectMetadata(bucketName, path).getUserMetadata();
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
			PutObjectRequest putObjectRequest = new PutObjectRequest(bucketName,
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
			ObjectListing ol = s3.listObjects(bucketName);
			List<S3ObjectSummary> objects = ol.getObjectSummaries();
			for (S3ObjectSummary os : objects) {
				if (os.getKey().startsWith("SubscriberMachines/")) {
					String data = os.getKey().replaceFirst("SubscriberMachines/", "");
					String[] dataA = data.split("/");
					if (dataA.length == 4) {
						// put object in map
						KeygroupID keygroupID = new KeygroupID(dataA[0], dataA[1], dataA[2]);
						String machine = dataA[3];
						Integer version = getVersionForSubscriberMachinePath(os.getKey());
						map.put(keygroupID, new Pair<String, Integer>(machine, version));
					} else {
						logger.warn("Could not read " + data);
					}
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
		ObjectListing ol = s3.listObjects(bucketName);
		List<S3ObjectSummary> objects = ol.getObjectSummaries();
		for (S3ObjectSummary os : objects) {
			if (os.getKey().startsWith(getSubscriberMachinesPath(keygroupID, ""))) {
				// here we have our object, now delete
				try {
					s3.deleteObject(bucketName, os.getKey());
				} catch (AmazonServiceException e) {
					throw new FBaseStorageConnectorException(e);
				}
			}
		}
	}

	private String getHeartbeatsPath(String machine) {
		return "SubscriberMachines/" + machine;
	}

	@Override
	public void heartbeats_update(String machine) throws FBaseStorageConnectorException {
		try {
			s3.putObject(bucketName, getHeartbeatsPath(machine),
					Long.toString(System.currentTimeMillis()));
		} catch (AmazonServiceException e) {
			throw new FBaseStorageConnectorException(e);
		}
	}

	@Override
	public Map<String, Long> heartbeats_getAll() throws FBaseStorageConnectorException {
		try {
			Map<String, Long> heartbeats = new HashMap<>();
			ObjectListing ol = s3.listObjects(bucketName);
			List<S3ObjectSummary> objects = ol.getObjectSummaries();
			for (S3ObjectSummary os : objects) {
				if (os.getKey().startsWith("SubscriberMachines/")) {
					BufferedReader reader = new BufferedReader(new InputStreamReader(
							s3.getObject(bucketName, os.getKey()).getObjectContent()));
					try {
						Long time = Long.parseLong(reader.readLine());
						heartbeats.put(os.getKey().replaceFirst("SubscriberMachines/", ""), time);
					} catch (NumberFormatException | IOException e) {
						logger.error("Cannot parse time from " + os.getKey(), e);
					}
				}
			}
			return heartbeats;
		} catch (AmazonServiceException e) {
			throw new FBaseStorageConnectorException(e);
		}
	}

	@Override
	public MessageID messageHistory_getNextMessageID() throws FBaseStorageConnectorException {
		// TODO 1: IMPLEMENT
		throw new RuntimeException("NOT YET IMPLEMENTED!");
	}

	@Override
	public void messageHistory_put(MessageID messageID, DataIdentifier relatedData)
			throws FBaseStorageConnectorException {
		// TODO 1: IMPLEMENT
		throw new RuntimeException("NOT YET IMPLEMENTED!");
	}

	@Override
	public DataIdentifier messageHistory_get(MessageID messageID)
			throws FBaseStorageConnectorException {
		// TODO 1: IMPLEMENT
		throw new RuntimeException("NOT YET IMPLEMENTED!");
	}

}
