package de.hasenburg.fbase.rest;

import java.io.IOException;
import java.io.PrintWriter;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.log4j.Logger;

import crypto.CryptoProvider;
import crypto.CryptoProvider.EncryptionAlgorithm;
import exceptions.FBaseRestException;
import model.config.KeygroupConfig;
import model.data.DataIdentifier;
import model.data.DataRecord;
import model.message.Message;

public class DataServlet extends HttpServlet {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private static Logger logger = Logger.getLogger(DataServlet.class.getName());

	@Override
	protected void doGet(HttpServletRequest req, HttpServletResponse resp)
			throws ServletException, IOException {		
		
		logger.debug("Received get request with query string " + req.getQueryString());
		
		PrintWriter w = resp.getWriter();
		DataIdentifier dataIdentifier = DataIdentifier.createFromString((req.getParameter("dataIdentifier")));
		Message m = new Message();
		
		try {
			if (dataIdentifier == null) {
				throw new FBaseRestException(FBaseRestException.DATAIDENTIFIER_MISSING, 400);
			}
			
			// TODO Temporary, replace with Database lookup
			KeygroupConfig config = new KeygroupConfig(dataIdentifier.getKeygroup(), "secret", EncryptionAlgorithm.AES);
			
			DataRecord record = new DataRecord();
			record.setDataIdentifier(dataIdentifier);
			record.setValue("Example value");
			// End Temporary
			
			if (record == null || config == null) {
				throw new FBaseRestException(FBaseRestException.NOT_FOUND, 404);
			}
			
			m.setTextualResponse("Success");
			m.setContent(CryptoProvider.encrypt(record.toJSON(), 
						config.getEncryptionSecret(), config.getEncryptionAlgorithm()));
			m.setContent(record.toJSON());
		} catch (FBaseRestException e) {
			logger.error(e.getMessage());
			resp.sendError(e.getHttpErrorCode(), e.getMessage());
		} catch (Exception e) {
			resp.sendError(500);
		}
		
		w.write(m.toJSON());
		resp.setStatus(200);
	}
	
	@Override
	protected void doPut(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
		
		/*
		PrintWriter w = resp.getWriter();
		String responseMessage = null;

		Keygroup keygroup = Keygroup.createFromString((req.getParameter("keygroup")));
		logger.info(keygroup);
		String request = req.getReader().lines().collect(Collectors.joining(System.lineSeparator()));
		
		if (keygroup == null) {
			// keygroup is missing
			responseMessage = ("Missing parameter keygroup");
			logger.warn(responseMessage);
		} else {	
			KeygroupConfiguration config = Mastermind.connector
					.getKeygroupConfiguration(keygroup);
			
			if (config == null) {
				responseMessage = ("No configuration for keygroup " + keygroup + " exists");
				logger.warn(responseMessage);
			} else {
				// decrypt data record
				String decryptedRequest = CryptoProvider.decrypt(request, config.getEncryptionSecret(), 
						config.getEncryptionAlgorithm());
				DataRecord record = DataRecord.createFromJSON(decryptedRequest);
				
				if (record == null) {
					// content is malformatted
					responseMessage = "Post reqeust has malformed content";
					logger.error(responseMessage);
				} else {
					// check if data is valid TODO 2: add more validation
					if (record.getKeygroup() == null) {
						responseMessage = "Did not create data record because it had no keygroup";
						logger.error(responseMessage);
					}
			
					// put data record into database
					DataIdentifier id = Mastermind.connector.putDataRecord(record);
					if (id != null) {
						responseMessage = "Created data record with ID " + id.toString();
						logger.info(responseMessage);
					} else {
						responseMessage = "Could not create data record, please check server log";
						logger.error(responseMessage);
					}
				}
			}
		}
		
		w.write(responseMessage);
		*/
	}
	
	@Override
	protected void doDelete(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
		// TODO Auto-generated method stub
		super.doDelete(req, resp);
	}
	
}
