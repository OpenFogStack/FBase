package de.hasenburg.fbase.rest;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.Set;
import java.util.stream.Collectors;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.log4j.Logger;

import com.fasterxml.jackson.databind.ObjectMapper;

import control.Mastermind;
import crypto.CryptoProvider;
import exceptions.FBaseRestException;
import exceptions.FBaseStorageConnectorException;
import model.config.KeygroupConfig;
import model.data.KeygroupID;
import model.message.Message;

public class RecordListServlet extends HttpServlet {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private static Logger logger = Logger.getLogger(RecordListServlet.class.getName());

	@Override
	protected void doGet(HttpServletRequest req, HttpServletResponse resp)
			throws ServletException, IOException {		
		
		logger.debug("Received get request with query string " + req.getQueryString());
		
		PrintWriter w = resp.getWriter();
		KeygroupID keygroupID = KeygroupID.createFromString((req.getParameter("keygroupID")));
		Message m = new Message();
		
		try {
			if (keygroupID == null) {
				throw new FBaseRestException(FBaseRestException.KEYGROUP_MISSING, 400);
			}
			
			KeygroupConfig config = null;
			String IDJson = null;
			
			try {
				config = Mastermind.connector.getKeygroupConfig(keygroupID);
				
				// create a set of dataidentifier strings
				Set<String> IDs = Mastermind.connector.listDataRecords(keygroupID)
						.stream().map(id -> id.toString()).collect(Collectors.toSet());
				
				ObjectMapper mapper = new ObjectMapper();
				IDJson = mapper.writeValueAsString(IDs); // if this fails, error 500 will be thrown
			} catch (FBaseStorageConnectorException e) {
				throw new FBaseRestException(FBaseRestException.NOT_FOUND, 404);
			}

			m.setTextualResponse("Success");
			m.setContent(CryptoProvider.encrypt(IDJson,
						config.getEncryptionSecret(), config.getEncryptionAlgorithm()));
			m.setContent(IDJson);
		} catch (FBaseRestException e) {
			logger.error(e.getMessage());
			resp.sendError(e.getHttpErrorCode(), e.getMessage());
		} catch (Exception e) {
			resp.sendError(500, "Plesase consult server log");
			e.printStackTrace();
		}
		
		w.write(m.toJSON());
		resp.setStatus(200);
	}
	
}
