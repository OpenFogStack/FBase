package client;

import java.io.IOException;
import java.util.List;

import org.apache.log4j.Logger;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.mashape.unirest.http.HttpResponse;
import com.mashape.unirest.http.Unirest;
import com.mashape.unirest.http.exceptions.UnirestException;

import exceptions.FBaseEncryptionException;
import model.JSONable;
import model.data.DataIdentifier;
import model.data.DataRecord;
import model.data.KeygroupID;
import model.messages.Message;

/**
 * A client implemantation which allows the usage of the FBase rest interface
 * 
 * @author jonathanhasenburg
 *
 */
public class Client {

	private static Logger logger = Logger.getLogger(Client.class.getName());
	
	public void getScenario(String address, int port) throws UnirestException {
		KeygroupID keygroupID = new KeygroupID("smartlight", "h1", "brightness");
		DataIdentifier dataID = new DataIdentifier(keygroupID, "M-1");
		
		DataRecord newRecord = new DataRecord(new DataIdentifier(keygroupID, "M-2"), null);
		newRecord.setValueWithoutKey("Example value");
		
		RecordRequest records = new RecordRequest(address, port);
		
		logger.info("Got M-1: " + records.getDataRecord(dataID));
		logger.info("Put M-2: " + records.putDataRecord(newRecord));
		logger.info("Got M-2: " + records.getDataRecord(newRecord.getDataIdentifier()));
		logger.info("List: " + records.listDataRecords(keygroupID));
		logger.info("Deleted M-2: " + records.deleteDataRecord(newRecord.getDataIdentifier()));
		logger.info("Got M-2: " + records.getDataRecord(newRecord.getDataIdentifier()));
		logger.info("List: " + records.listDataRecords(keygroupID));
	}
	
	public static void main(String[] args) throws UnirestException, FBaseEncryptionException {
		String address = "localhost";
		int port = 8081;
		
		Client c = new Client();
		c.getScenario(address, port);
	}



//	public boolean keygroupConfig_create(String address, int port, KeygroupConfig keygroupConfig)
//			throws UnirestException, FBaseEncryptionException {
//		
//		// prepare
//		String target = address + ":" + port + "/keygroupConfig";
//		logger.info("Running post request targeting " + target);
//		
//		// create and sign request message
//		Message requestM = new Message();
//		requestM.setContent(JSONable.toJSON(keygroupConfig));
//		requestM.signMessage(privateKey, algorithm);
//
//		// send message
//		HttpResponse<String> response = Unirest.post(target).header("accept", "application/json")
//				.queryString("clientID", clientID.getID()).body(JSONable.toJSON(requestM))
//				.asString();
//
//		// process response
//		if (response.getStatus() == 200) {
//			return true;
//		} else {
//			logger.error("Status = " + response.getStatus());
//			return false;
//		}
//	}

}
