package storageconnector;

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
import com.amazonaws.services.s3.model.S3ObjectSummary;

import exceptions.FBaseStorageConnectorException;
import model.JSONable;
import model.config.ClientConfig;
import model.config.KeygroupConfig;
import model.config.NodeConfig;
import model.data.ClientID;
import model.data.DataIdentifier;
import model.data.DataRecord;
import model.data.KeygroupID;
import model.data.NodeID;

public class S3DBConnector extends AbstractDBConnector {

	private static final Logger logger = Logger.getLogger(S3DBConnector.class);

	private final String bucketName = "de.hasenburg.fbase.s3dbconnector-bucket";
	private AmazonS3 s3;

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

	@Override
	public void keygroupConfig_put(KeygroupID keygroupID, KeygroupConfig config)
			throws FBaseStorageConnectorException {
		// TODO Auto-generated method stub

	}

	@Override
	protected KeygroupConfig keygroupConfig_get(KeygroupID keygroupID)
			throws FBaseStorageConnectorException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void nodeConfig_put(NodeID nodeID, NodeConfig config)
			throws FBaseStorageConnectorException {
		// TODO Auto-generated method stub

	}

	@Override
	protected NodeConfig nodeConfig_get(NodeID nodeID) throws FBaseStorageConnectorException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void clientConfig_put(ClientID clientID, ClientConfig config)
			throws FBaseStorageConnectorException {
		// TODO Auto-generated method stub

	}

	@Override
	protected ClientConfig clientConfig_get(ClientID clientID)
			throws FBaseStorageConnectorException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Integer keyGroupSubscriberMachines_put(KeygroupID keygroup, String machine)
			throws FBaseStorageConnectorException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Map<KeygroupID, Pair<String, Integer>> keyGroupSubscriberMachines_listAll()
			throws FBaseStorageConnectorException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void keyGroupSubscriberMachines_remove(KeygroupID keygroupid)
			throws FBaseStorageConnectorException {
		// TODO Auto-generated method stub

	}

	@Override
	public void heartbeats_update(String machine) throws FBaseStorageConnectorException {
		// TODO Auto-generated method stub

	}

	@Override
	public Map<String, Long> heartbeats_getAll() throws FBaseStorageConnectorException {
		// TODO Auto-generated method stub
		return null;
	}

}
