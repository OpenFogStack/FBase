package client;

import java.util.List;

import org.apache.log4j.Logger;

import com.mashape.unirest.http.HttpResponse;
import com.mashape.unirest.http.Unirest;
import com.mashape.unirest.http.exceptions.UnirestException;

import model.JSONable;
import model.data.DataIdentifier;
import model.data.DataRecord;
import model.data.KeygroupID;

public class RecordRequest extends RessourceRequest {

	private static final String PATH = "records";
	private static Logger logger = Logger.getLogger(RecordRequest.class.getName());

	public RecordRequest(String address, int port) {
		super(address, port, PATH);
	}

	public DataRecord getDataRecord(DataIdentifier dataIdentifier) throws UnirestException {
		String target = target(dataIdentifier.toString());
		logger.info("Running get request targeting " + target);

		HttpResponse<String> response = Unirest.get(target).asString();
		return handleObjectResponse(response, DataRecord.class, logger);
	}

	public boolean putDataRecord(DataRecord dataRecord) throws UnirestException {
		String target = target();
		logger.info("Running put request targeting " + target);

		HttpResponse<String> response =
				Unirest.put(target).header("Content-Type", "application/json")
						.body(JSONable.toJSON(dataRecord)).asString();

		return handleBoolResponse(response, logger);
	}

	public List<DataIdentifier> listDataRecords(KeygroupID keygroupID) throws UnirestException {
		String target = target(keygroupID.toString());
		logger.info("Running list request targeting " + target);

		HttpResponse<String> response = Unirest.get(target).asString();
		return handleListObjectResponse(response, DataIdentifier.class, logger);
	}
	
	public boolean deleteDataRecord(DataIdentifier dataIdentifier) throws UnirestException {
		String target = target(dataIdentifier.toString());
		logger.info("Running get request targeting " + target);

		HttpResponse<String> response = Unirest.delete(target).asString();
		return handleBoolResponse(response, logger);
	}
}
