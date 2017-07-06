package de.hasenburg.fbase.rest;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.Set;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.log4j.Logger;

import control.Mastermind;
import crypto.CryptoProvider;
import exceptions.FBaseRestException;
import exceptions.FBaseStorageConnectorException;
import model.config.KeygroupConfig;
import model.data.DataIdentifier;
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
			String IDString = null;
			
			try {
				config = Mastermind.connector.getKeygroupConfig(keygroupID);
				Set<DataIdentifier> IDs = Mastermind.connector.listDataRecords(keygroupID);
				IDString = "[";
				for (DataIdentifier di : IDs) {
					IDString += "\"" + di.toString() + "\", "; // TODO no , at the end
				}
				IDString += "]";
			} catch (FBaseStorageConnectorException e) {
				throw new FBaseRestException(FBaseRestException.NOT_FOUND, 404);
			}

			m.setTextualResponse("Success");
			m.setContent(CryptoProvider.encrypt(IDString,
						config.getEncryptionSecret(), config.getEncryptionAlgorithm()));
			m.setContent(IDString);
		} catch (FBaseRestException e) {
			logger.error(e.getMessage());
			resp.sendError(e.getHttpErrorCode(), e.getMessage());
		} catch (Exception e) {
			resp.sendError(500);
			e.printStackTrace();
		}
		
		w.write(m.toJSON());
		resp.setStatus(200);
	}
	
}
