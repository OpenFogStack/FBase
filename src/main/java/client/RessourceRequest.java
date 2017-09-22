package client;

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONObject;

import com.mashape.unirest.http.HttpResponse;

import model.JSONable;
import model.messages.Message;

public abstract class RessourceRequest {

	private String address;
	private int port;

	private String path;

	public RessourceRequest(String address, int port, String path) {
		this.address = address;
		this.port = port;
		this.path = path;
	}

	protected String target() {
		return "http://" + address + ":" + port + "/jersey/" + path;
	}

	protected String target(String a) {
		return target() + "/" + a;
	}

	protected String target(String a, String b) {
		return target(a) + "/" + b;
	}

	protected String target(String a, String b, String c) {
		return target(a, b) + "/" + c;
	}

	protected <T> T handleObjectResponse(HttpResponse<String> response, Class<T> targetClass,
			Logger logger) {
		T returnObject = null;
		if (response.getStatus() == 200) {
			logger.info("Status = 200");
			Message m = JSONable.fromJSON(response.getBody(), Message.class);
			returnObject = JSONable.fromJSON(m.getContent(), targetClass);
		} else {
			logger.error("Status = " + response.getStatus());
			logger.error("Message = " + response.getStatusText());
		}
		return returnObject;
	}

	protected boolean handleBoolResponse(HttpResponse<String> response, Logger logger) {
		if (response.getStatus() == 200) {
			logger.info("Status = 200");
			return true;
		} else {
			logger.error("Status = " + response.getStatus());
			logger.error("Message = " + response.getStatusText());
		}
		return false;
	}
	
	protected <T> List<T> handleListObjectResponse(HttpResponse<String> response, Class<T> targetClass,
			Logger logger) {
		List<T> returnObjects = new ArrayList<>();
		if (response.getStatus() == 200) {
			logger.info("Status = 200");
			Message m = JSONable.fromJSON(response.getBody(), Message.class);
			
			JSONArray jsonArray = new JSONArray(m.getContent());
			for (int i = 0; i < jsonArray.length(); i++) {
				JSONObject jsonObject = jsonArray.getJSONObject(i);
				T returnObject = JSONable.fromJSON(jsonObject.toString(), targetClass);
				returnObjects.add(returnObject);
			}
				
		} else {
			logger.error("Status = " + response.getStatus());
			logger.error("Message = " + response.getStatusText());
		}
		return returnObjects;
	}

}
