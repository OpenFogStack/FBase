package de.hasenburg.fbase.rest;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.log4j.Logger;

import control.FBase;
import crypto.CryptoProvider;
import exceptions.FBaseRestException;
import exceptions.FBaseStorageConnectorException;
import model.JSONable;
import model.config.KeygroupConfig;
import model.data.DataIdentifier;
import model.data.DataRecord;
import model.data.KeygroupID;
import model.messages.Message;

/**
 * 
 * @author jonathanhasenburg
 *
 */
public class RecordServlet extends HttpServlet {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private static Logger logger = Logger.getLogger(RecordServlet.class.getName());
	private FBase fBase = null;

	public RecordServlet(FBase fBase) {
		this.fBase = fBase;
	}

	@Override
	protected void doGet(HttpServletRequest req, HttpServletResponse resp)
			throws ServletException, IOException {

		logger.debug("Received get request with query string " + req.getQueryString());

		PrintWriter w = resp.getWriter();
		DataIdentifier dataIdentifier =
				DataIdentifier.createFromString((req.getParameter("dataIdentifier")));
		Message m = new Message();

		try {
			if (dataIdentifier == null) {
				// 400 Bad Request
				throw new FBaseRestException(FBaseRestException.DATAIDENTIFIER_MISSING, 400);
			}

			KeygroupConfig config = null;
			DataRecord record = null;
			try {
				config = fBase.configAccessHelper.keygroupConfig_get(dataIdentifier.getKeygroupID())
						.getValue0();
				record = fBase.connector.dataRecords_get(dataIdentifier);
				if (config == null || record == null) {
					// 404 Not Found
					throw new FBaseRestException(FBaseRestException.NOT_FOUND, 404);
				}
			} catch (FBaseStorageConnectorException e) {
				// 404 Not Found
				throw new FBaseRestException(FBaseRestException.NOT_FOUND, 404);
			}

			// 200 OK
			resp.setStatus(200);
			m.setTextualInfo("Success");
			m.setContent(CryptoProvider.encrypt(JSONable.toJSON(record),
					config.getEncryptionSecret(), config.getEncryptionAlgorithm()));
			m.setContent(JSONable.toJSON(record)); // remove to encrypt
			w.write(JSONable.toJSON(m));
		} catch (FBaseRestException e) {
			logger.error(e.getMessage());
			resp.sendError(e.getHttpErrorCode(), e.getMessage());
		} catch (Exception e) {
			// 500 Internal Server Error
			resp.sendError(500);
			e.printStackTrace();
		}

	}

	@Override
	protected void doPut(HttpServletRequest req, HttpServletResponse resp)
			throws ServletException, IOException {

		logger.debug("Received put request with query string " + req.getQueryString());

		PrintWriter w = resp.getWriter();
		KeygroupID keygroupID = KeygroupID.createFromString((req.getParameter("keygroupID")));
		Message m = new Message();

		try {
			if (keygroupID == null) {
				// 400 Bad Request
				throw new FBaseRestException(FBaseRestException.KEYGROUP_MISSING, 400);
			}

			KeygroupConfig config = null;
			try {
				config = fBase.configAccessHelper.keygroupConfig_get(keygroupID).getValue0();
				if (config == null) {
					// 404 Not Found
					throw new FBaseRestException(FBaseRestException.NOT_FOUND, 404);
				}
			} catch (FBaseStorageConnectorException e) {
				// 404 Not Found
				throw new FBaseRestException(FBaseRestException.NOT_FOUND, 404);
			}

			// decrypt data record
			String body =
					req.getReader().lines().collect(Collectors.joining(System.lineSeparator()));
			// String decryptedRequest = CryptoProvider.decrypt(body,
			// config.getEncryptionSecret(),
			// config.getEncryptionAlgorithm());
			String decryptedRequest = body; // Remove to decrypt
			DataRecord record = JSONable.fromJSON(decryptedRequest, DataRecord.class);
			if (record == null) {
				// 400 Bad Request
				throw new FBaseRestException(FBaseRestException.BODY_NOT_PARSEABLE, 400);
			}

			// store data record
			Future<Boolean> future = fBase.taskmanager.runPutDataRecordTask(record);

			// 404 Not Found
			boolean success = future.get(5, TimeUnit.SECONDS);
			if (!success)
				throw new FBaseRestException(FBaseRestException.NOT_FOUND, 404);

			// 200 OK
			resp.setStatus(200);
			m.setTextualInfo("Success");
			w.write(JSONable.toJSON(m));
		} catch (FBaseRestException e) {
			logger.error(e.getMessage());
			resp.sendError(e.getHttpErrorCode(), e.getMessage());
		} catch (Exception e) {
			// 500 Internal Server Error
			resp.sendError(500);
			e.printStackTrace();
		}
	}

	@Override
	protected void doDelete(HttpServletRequest req, HttpServletResponse resp)
			throws ServletException, IOException {

		logger.debug("Received delete request with query string " + req.getQueryString());

		PrintWriter w = resp.getWriter();
		KeygroupID keygroupID = KeygroupID.createFromString((req.getParameter("keygroupID")));
		Message m = new Message();

		try {
			if (keygroupID == null) {
				// 400 Bad Request
				throw new FBaseRestException(FBaseRestException.KEYGROUP_MISSING, 400);
			}

			KeygroupConfig config = null;
			try {
				config = fBase.configAccessHelper.keygroupConfig_get(keygroupID).getValue0();
				if (config == null) {
					// 404 Not Found
					throw new FBaseRestException(FBaseRestException.NOT_FOUND, 404);
				}
			} catch (FBaseStorageConnectorException e) {
				// 404 Not Found
				throw new FBaseRestException(FBaseRestException.NOT_FOUND, 404);
			}

			// decrypt data record
			String body =
					req.getReader().lines().collect(Collectors.joining(System.lineSeparator()));
			// String decryptedRequest = CryptoProvider.decrypt(body,
			// config.getEncryptionSecret(),
			// config.getEncryptionAlgorithm());
			String decryptedRequest = body; // Remove to decrypt
			DataIdentifier dataIdentifier =
					JSONable.fromJSON(decryptedRequest, DataIdentifier.class);
			if (dataIdentifier == null) {
				// 400 Bad Request
				throw new FBaseRestException(FBaseRestException.BODY_NOT_PARSEABLE, 400);
			}

			Future<Boolean> future = fBase.taskmanager.runDeleteDataRecordTask(dataIdentifier, true);

			// 404 Not Found
			boolean success = future.get(5, TimeUnit.SECONDS);
			if (!success)
				throw new FBaseRestException(FBaseRestException.NOT_FOUND, 404);

			// 200 OK
			resp.setStatus(200);
			m.setTextualInfo("Success");
			w.write(JSONable.toJSON(m));
		} catch (FBaseRestException e) {
			logger.error(e.getMessage());
			resp.sendError(e.getHttpErrorCode(), e.getMessage());
		} catch (Exception e) {
			// 500 Internal Server Error
			resp.sendError(500);
			e.printStackTrace();
		}
	}

}
