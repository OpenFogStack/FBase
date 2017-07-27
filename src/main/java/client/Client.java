package client;

import java.io.IOException;
import java.util.List;

import org.apache.log4j.Logger;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.mashape.unirest.http.HttpResponse;
import com.mashape.unirest.http.Unirest;
import com.mashape.unirest.http.exceptions.UnirestException;

import model.JSONable;
import model.data.DataIdentifier;
import model.data.DataRecord;
import model.data.KeygroupID;
import model.messages.datarecords.Message;

/**
 * A client implemantation which allows the usage of the FBase rest interface
 * 
 * @author jonathanhasenburg
 *
 */
public class Client {

	private static Logger logger = Logger.getLogger(Client.class.getName());

	public static void main(String[] args) throws UnirestException {
		Client c = new Client();
		DataRecord record = new DataRecord();
		DataIdentifier dataIdentifier = new DataIdentifier("smartlight", "h1", "brightness", "M-1");
		record.setDataIdentifier(new DataIdentifier(dataIdentifier.getKeygroupID(), "M-4"));
		Message m = c.runPutRecordRequest("http://localhost", 8080, record);
		logger.info(JSONable.toJSON(m));

		DataRecord record2 = c.runGetRecordRequest("http://localhost", 8080, dataIdentifier);
		logger.info(JSONable.toJSON(record2));

		List<String> list = c.runGetListRecordRequest("http://localhost", 8080,
				dataIdentifier.getKeygroupID());
		list.stream().forEach(i -> logger.info(i));
	}

	public Client() {
	}

	public DataRecord runGetRecordRequest(String address, int port, DataIdentifier dataIdentifier)
			throws UnirestException {
		DataRecord record = null;
		try {
			String target = address + ":" + port + "/record";
			logger.info("Running get request targeting " + target);
			HttpResponse<String> response = Unirest.get(target)
					.queryString("dataIdentifier", dataIdentifier).asString();
			if (response.getStatus() == 200) {
				logger.info("Status = 200");
				Message m = JSONable.fromJSON(response.getBody(), Message.class);
				// Insert decryption here if needed
				record = JSONable.fromJSON(m.getContent(), DataRecord.class);
			} else {
				logger.info("Status = " + response.getStatus());
			}
		} catch (UnirestException e) {
			e.printStackTrace();
		}

		return record;
	}

	@SuppressWarnings("unchecked")
	public List<String> runGetListRecordRequest(String address, int port, KeygroupID keygroupID) {
		List<String> list = null;
		try {
			String target = address + ":" + port + "/record/list";
			logger.info("Running get request targeting " + target);
			HttpResponse<String> response = Unirest.get(target)
					.queryString("keygroupID", keygroupID).asString();
			if (response.getStatus() == 200) {
				logger.info("Status = 200");
				Message m = JSONable.fromJSON(response.getBody(), Message.class);
				// Insert decryption here if needed
				ObjectMapper mapper = new ObjectMapper();
				list = mapper.readValue(m.getContent(), List.class);
			} else {
				logger.info("Status = " + response.getStatus());
			}
		} catch (UnirestException | IOException e) {
			e.printStackTrace();
		}
		return list;
	}

	public Message runPutRecordRequest(String address, int port, DataRecord record) {
		Message m = null;
		try {
			String target = address + ":" + port + "/record";
			logger.info("Running put request targeting " + target);
			HttpResponse<String> response = Unirest.put(target).header("accept", "application/json")
					// insert decryption here if needed
					.queryString("keygroupID", record.getKeygroupID()).body(JSONable.toJSON(record))
					.asString();
			if (response.getStatus() == 200) {
				m = JSONable.fromJSON(response.getBody(), Message.class);
			} else {
				logger.info("Status = " + response.getStatus());
			}
		} catch (UnirestException e) {
			e.printStackTrace();
		}
		return m;
	}

}
