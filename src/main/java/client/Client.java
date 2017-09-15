package client;

import java.io.IOException;
import java.util.List;

import org.apache.log4j.Logger;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.mashape.unirest.http.HttpResponse;
import com.mashape.unirest.http.Unirest;
import com.mashape.unirest.http.exceptions.UnirestException;

import crypto.CryptoProvider.EncryptionAlgorithm;
import exceptions.FBaseEncryptionException;
import model.JSONable;
import model.config.KeygroupConfig;
import model.data.ClientID;
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

	private ClientID clientID = null;
	private String publicKey = "MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAnvVCGNNQMcI9FCio5Hhu0JNW3kXTYj+XqmuFolQyNj+kbEZgZ718i2ujRoz5PSKU8oWSwiDKDxHmhnEAwvwX2w4E/p74SSWuv18FLcobBVad4QUbJFLCp1+JmPLJuBN/Vjvke3RYUgZcZifaH8OQDaAnJsWrNlGHRjafXwFxiwHD6KA6J8mW7y+xtcS3WDsstAwVrxZMkENnkEg3syCOKPsaUuUQgo/yE2GCaJd41v8rGee+V7ThDcVqyAJpWi/tDGclEJvH2HrPsxzUnY0cl10OAkzzZiF7lWIr/cGWpRvlzygeHqT538mLckImFRwxF4EVU/N5AoDDPhWdhNCy4QIDAQAB";
	private String privateKey = "MIIEvQIBADANBgkqhkiG9w0BAQEFAASCBKcwggSjAgEAAoIBAQCe9UIY01Axwj0UKKjkeG7Qk1beRdNiP5eqa4WiVDI2P6RsRmBnvXyLa6NGjPk9IpTyhZLCIMoPEeaGcQDC/BfbDgT+nvhJJa6/XwUtyhsFVp3hBRskUsKnX4mY8sm4E39WO+R7dFhSBlxmJ9ofw5ANoCcmxas2UYdGNp9fAXGLAcPooDonyZbvL7G1xLdYOyy0DBWvFkyQQ2eQSDezII4o+xpS5RCCj/ITYYJol3jW/ysZ575XtOENxWrIAmlaL+0MZyUQm8fYes+zHNSdjRyXXQ4CTPNmIXuVYiv9wZalG+XPKB4epPnfyYtyQiYVHDEXgRVT83kCgMM+FZ2E0LLhAgMBAAECggEAKWDmx9eaNJm/lJPRA0hmcF0qa8n5cMXlETPUIaGRQJXok1bumZK17QUkB6WC6+sooskqnuYyMyIRxEV+UUOWYiuQGUgAkeoBR0fQ7U6DIiyeBIlzu0zf5vnzs6Df6c8JgsUsgQaURyXov574JPSfdyyNhIZIP2eRszFc4zpY+Dodn5c8ZTVNY4aXan65ZGvYPYwU3aRzkJg325YdwYgbeLs78TyeZ1MoZrYG2cNrZfR6I4hZeZXCN5VBL1GBa/bycy/c9tIQQlfS3tpwR1Y3pmcn6gL0ow2wMRrYoiEy1OxCGb1E9uobi2mUbdPAC86USjjdpSSfWPiuOaEjtIl20QKBgQD9nb2+yFlA/z5cimCXmi839JLCPpIL+KwBKK0tQuqfo6JHJZEusaIwg19rtcWlaR8OAHvXPT7p2FdwaNnUKPwZHav1vwzubTvLtxVTEeZZrOvgW+WQhKk6qlBd3StiLq70Ubp50BjrT9aVRCcYPDDWvUE5ohqOIa6dBKfXBTNeAwKBgQCgc79trki9w/Ig/ilNfjkkzkt96QYf+9V1HQbh/OCu8YdVPaVHfydDPsKPeAXJ7MRSR6mIHgOBAJ3s25tWJUCTnYt2xChIdMnkQH8V9PSBx/JQYtWcDY1VSMfUiQADCA5ca/iONybhZuBL9ma3Lhz9qhAzmVLgtdf70Cg/ima4SwKBgG8UR6bn2S++m3GsqeG8hjHbzOuGvDDCGZPoXPEV/e0tnkXLDmuFIaRd5c0nMAnioNdhHtyG1qdVOfbh9YYW7VOSy14271L+RNJUveJEVL+yHR2HImTJtdUcA1cZJ4c5KyeXJDV2D3QA49s8nmLe+gUTnx2/AiJ+Xhwnjdt/S6BBAoGAYsK8GnPDPGLmn14x6AwAemIsX2TWK0ukOMDUIre3SJdMGLCoEhj2/tIbiZlz3rVIpeiMNkdbGsVZb5hAxcaKOBIp7MGSlf6k4kS8tLQg5909jjM3jiVdUBhLP8vP4Q3NYR/oTwktemILP1Z8JNZSa+SIsmn2dHAZcFrQ20OCNH8CgYEAp5vLBhuI8Hwz82NfoO8JOQGuk5HKFIxf0w4TgmwYk1wyugJiqhFx9V/KnXHSvFAv+4BzGZFMTafGGFWxluHjvaHS/XoH/q9Jkd4SlkqvXyI8AcwMxilVT5HPySkz5dM5cvaC54v4WnoZZSB2yg5+538xr12piUQ+H9P4l5yxfg8=";
	private EncryptionAlgorithm algorithm = EncryptionAlgorithm.RSA;
	
	public static void main(String[] args) throws UnirestException, FBaseEncryptionException {
		Client c = new Client(new ClientID("C-1"));
		
		String address = "http://localhost";
		int port = 8081;
		
		KeygroupConfig keygroupConfig = new KeygroupConfig();
		keygroupConfig.setKeygroupID(new KeygroupID("smartlight", "h1", "brightness"));
				
		logger.debug(c.keygroupConfig_create(address, port, keygroupConfig));
		
//		DataRecord record = new DataRecord();
//		DataIdentifier dataIdentifier = new DataIdentifier("smartlight", "h1", "brightness", "M-1");
//		record.setDataIdentifier(new DataIdentifier(dataIdentifier.getKeygroupID(), "M-4"));
//		Message m = c.runPutRecordRequest("http://localhost", 8080, record);
//		logger.info(JSONable.toJSON(m));
//
//		DataRecord record2 = c.runGetRecordRequest("http://localhost", 8080, dataIdentifier);
//		logger.info(JSONable.toJSON(record2));
//
//		List<String> list =
//				c.runGetListRecordRequest("http://localhost", 8080, dataIdentifier.getKeygroupID());
//		list.stream().forEach(i -> logger.info(i));
	}

	public Client(ClientID clientID) {
		this.clientID = clientID;
	}

	public boolean keygroupConfig_create(String address, int port, KeygroupConfig keygroupConfig)
			throws UnirestException, FBaseEncryptionException {
		
		// prepare
		String target = address + ":" + port + "/keygroupConfig";
		logger.info("Running post request targeting " + target);
		
		// create and sign request message
		Message requestM = new Message();
		requestM.setContent(JSONable.toJSON(keygroupConfig));
		requestM.signMessage(privateKey, algorithm);

		// send message
		HttpResponse<String> response = Unirest.post(target).header("accept", "application/json")
				.queryString("clientID", clientID.getID()).body(JSONable.toJSON(requestM))
				.asString();

		// process response
		if (response.getStatus() == 200) {
			return true;
		} else {
			logger.error("Status = " + response.getStatus());
			return false;
		}
	}

	public DataRecord runGetRecordRequest(String address, int port, DataIdentifier dataIdentifier)
			throws UnirestException {
		DataRecord record = null;
		try {
			String target = address + ":" + port + "/record";
			logger.info("Running get request targeting " + target);
			HttpResponse<String> response =
					Unirest.get(target).queryString("dataIdentifier", dataIdentifier).asString();
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
			HttpResponse<String> response =
					Unirest.get(target).queryString("keygroupID", keygroupID).asString();
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
					.queryString("keygroupID", record.getKeygroupID().getID())
					.body(JSONable.toJSON(record)).asString();
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

	public Message runDeleteRecordRequest(String address, int port, DataIdentifier identifier) {
		Message m = null;
		try {
			String target = address + ":" + port + "/record";
			logger.info("Running delete request targeting " + target);
			HttpResponse<String> response =
					Unirest.delete(target).header("accept", "application/json")
							.queryString("keygroupID", identifier.getKeygroupID().getID())
							.body(JSONable.toJSON(identifier)) // insert encryption here if
																// needed
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
