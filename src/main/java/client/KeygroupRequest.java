package client;

import model.data.NodeID;
import org.apache.log4j.Logger;

import com.mashape.unirest.http.HttpResponse;
import com.mashape.unirest.http.Unirest;
import com.mashape.unirest.http.exceptions.UnirestException;

import model.JSONable;
import model.config.KeygroupConfig;
import model.config.ReplicaNodeConfig;
import model.config.TriggerNodeConfig;
import model.data.ClientID;
import model.data.KeygroupID;

public class KeygroupRequest extends RessourceRequest {

	private static final String PATH = "keygroups";
	private static Logger logger = Logger.getLogger(KeygroupRequest.class.getName());

	public KeygroupRequest(String address, int port) {
		super(address, port, PATH);
	}

	public KeygroupConfig updateLocalKeygroupConfig(KeygroupID keygroupID) throws UnirestException {
		String target = target(keygroupID.getApp(), keygroupID.getTenant(), keygroupID.getGroup(), "update");
		logger.debug("Running get request targeting " + target);

		HttpResponse<String> response =
				Unirest.get(target).asString();

		return handleObjectResponse(response, KeygroupConfig.class, logger);
	}
	
	public boolean createKeygroup(KeygroupConfig keygroupConfig) throws UnirestException {
		String target = target();
		logger.debug("Running post request targeting " + target);

		HttpResponse<String> response =
				Unirest.post(target).header("Content-Type", "application/json")
						.body(JSONable.toJSON(keygroupConfig)).asString();

		return handleBoolResponse(response, logger);
	}
	
	public boolean addClient(KeygroupID keygroupID, ClientID clientID) throws UnirestException {
		String target = target(keygroupID.getApp(), keygroupID.getTenant(), keygroupID.getGroup(), "addClient");
		logger.debug("Running put request targeting " + target);

		HttpResponse<String> response =
				Unirest.put(target).header("Content-Type", "application/json")
						.body(JSONable.toJSON(clientID)).asString();

		return handleBoolResponse(response, logger);
	}

	public boolean deleteClient(KeygroupID keygroupID, ClientID clientID) throws UnirestException {
		String target = target(keygroupID.getApp(), keygroupID.getTenant(), keygroupID.getGroup(), "deleteClient");
		logger.debug("Running delete request targeting " + target);

		HttpResponse<String> response =
				Unirest.put(target).header("Content-Type", "application/json")
					   .body(JSONable.toJSON(clientID)).asString();

		return handleBoolResponse(response, logger);
	}
	
	public boolean addReplicaNode(KeygroupID keygroupID, ReplicaNodeConfig repNC) throws UnirestException {
		String target = target(keygroupID.getApp(), keygroupID.getTenant(), keygroupID.getGroup(), "addReplicaNode");
		logger.debug("Running put request targeting " + target);

		HttpResponse<String> response =
				Unirest.put(target).header("Content-Type", "application/json")
						.body(JSONable.toJSON(repNC)).asString();

		return handleBoolResponse(response, logger);
	}
	
	public boolean addTriggerNode(KeygroupID keygroupID, TriggerNodeConfig triggerNC) throws UnirestException {
		String target = target(keygroupID.getApp(), keygroupID.getTenant(), keygroupID.getGroup(), "addTriggerNode");
		logger.debug("Running put request targeting " + target);

		HttpResponse<String> response =
				Unirest.put(target).header("Content-Type", "application/json")
						.body(JSONable.toJSON(triggerNC)).asString();

		return handleBoolResponse(response, logger);
	}

	public boolean deleteNode(KeygroupID keygroupID, NodeID nodeID) throws UnirestException {
		String target = target(keygroupID.getApp(), keygroupID.getTenant(), keygroupID.getGroup(), "deleteNode");
		logger.debug("Running delete request targeting " + target);

		HttpResponse<String> response =
				Unirest.put(target).header("Content-Type", "application/json")
					   .body(JSONable.toJSON(nodeID)).asString();

		return handleBoolResponse(response, logger);
	}
}
