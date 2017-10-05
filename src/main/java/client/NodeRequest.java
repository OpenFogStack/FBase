package client;

import org.apache.log4j.Logger;

import com.mashape.unirest.http.HttpResponse;
import com.mashape.unirest.http.Unirest;
import com.mashape.unirest.http.exceptions.UnirestException;

import model.JSONable;
import model.config.NodeConfig;

public class NodeRequest extends RessourceRequest {

	private static final String PATH = "nodes";
	private static Logger logger = Logger.getLogger(NodeRequest.class.getName());

	public NodeRequest(String address, int port) {
		super(address, port, PATH);
	}

	public NodeConfig getNodeConfig() throws UnirestException {
		String target = target();
		logger.info("Running get request targeting " + target);

		HttpResponse<String> response = Unirest.get(target).asString();
		return handleObjectResponse(response, NodeConfig.class, logger);
	}

	public boolean createNodeConfig(NodeConfig nodeConfig) throws UnirestException {
		String target = target();
		logger.info("Running post request targeting " + target);

		HttpResponse<String> response =
				Unirest.post(target).header("Content-Type", "application/json")
						.body(JSONable.toJSON(nodeConfig)).asString();

		return handleBoolResponse(response, logger);
	}
	
}
