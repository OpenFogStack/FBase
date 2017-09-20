package de.hasenburg.fbase.rest;

import java.io.IOException;
import java.io.PrintWriter;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.log4j.Logger;

import control.FBase;
import crypto.AlgorithmAES;
import crypto.CryptoProvider.EncryptionAlgorithm;
import exceptions.FBaseRestException;
import model.JSONable;
import model.config.ClientConfig;
import model.config.KeygroupConfig;
import model.data.ClientID;
import model.data.KeygroupID;
import model.messages.Message;

/**
 * 
 * This servlet offers clients the ability to perform CRD operations for
 * {@link KeygroupConfig}s.
 * 
 * @author jonathanhasenburg
 *
 */
public class KeygroupConfigServlet extends HttpServlet {

	private static final long serialVersionUID = 1L;
	private static Logger logger = Logger.getLogger(KeygroupConfigServlet.class.getName());
	private final FBase fBase;

	public KeygroupConfigServlet(FBase fBase) {
		this.fBase = fBase;
	}

	@Override
	protected void doGet(HttpServletRequest req, HttpServletResponse resp)
			throws ServletException, IOException {
		// prepare
		Message response = new Message();
		ServletHelperMethods.logRequest(logger, req);
		PrintWriter w = resp.getWriter();

		try {
			// get query parameters (returns 400)
			KeygroupID keygroupID = ServletHelperMethods.parseParameter(req, new KeygroupID());
			ClientID clientID = ServletHelperMethods.parseParameter(req, new ClientID());

			// gather configs (returns 404)
			ClientConfig clientConfig = ServletHelperMethods.getConfig(clientID, fBase);
			KeygroupConfig keygroupConfig = ServletHelperMethods.getConfig(keygroupID, fBase);

			// prepare response
			if (keygroupConfig.getClients().contains(clientID)) {
				// return full but encrypted
				response.setContent(JSONable.toJSON(keygroupConfig));
				response.encryptFields(clientConfig.getPublicKey(),
						clientConfig.getEncryptionAlgorithm());
			} else {
				// return without secrets and not encrypted
				// FIXME heap connector does return references, so we need to protect database
				KeygroupConfig configReturn = JSONable.clone(keygroupConfig);
				configReturn.setEncryptionAlgorithm(null);
				configReturn.setEncryptionSecret(null);
				response.setContent(JSONable.toJSON(keygroupConfig));
			}
		} catch (FBaseRestException e) {
			logger.warn(e.getMessage(), e);
			resp.sendError(e.getHttpErrorCode(), e.getMessage());
		} catch (Exception e) {
			logger.warn(e);
			resp.sendError(500);
		}

		resp.setStatus(200);
		w.write(JSONable.toJSON(response));
	}

	@Override
	protected void doPost(HttpServletRequest req, HttpServletResponse resp)
			throws ServletException, IOException {
		// prepare
		ServletHelperMethods.logRequest(logger, req);

		try {
			// get query parameters (returns 400)
			ClientID clientID = ServletHelperMethods.parseParameter(req, new ClientID());

			// gather configs (returns 404)
			ClientConfig clientConfig = ServletHelperMethods.getConfig(clientID, fBase);

			// parse and verify body (returns 400, 401)
			Message m = ServletHelperMethods.parseAndVerifyBody(req, clientConfig);

			// read in KeygroupConfig (returns 400)
			KeygroupConfig config = JSONable.fromJSON(m.getContent(), KeygroupConfig.class);
			if (config == null) {
				throw new FBaseRestException(FBaseRestException.BODY_NOT_PARSEABLE, 400);
			}

			// assign a secret, an algorithm and the client
			config.setEncryptionSecret(AlgorithmAES.generateNewSecret());
			config.setEncryptionAlgorithm(EncryptionAlgorithm.AES);
			config.addClient(clientID);

			// create at namingservice and store 
			KeygroupConfig approvedConfig =
					fBase.namingServiceSender.sendKeygroupConfigCreate(config);
			if (approvedConfig != null) {
				fBase.taskmanager.runUpdateKeygroupConfigTask(approvedConfig, false);
			} else {
				throw new FBaseRestException(FBaseRestException.KEYGROUP_ALREADY_EXISTS, 412);
			}

		} catch (FBaseRestException e) {
			logger.error(e.getMessage());
			resp.sendError(e.getHttpErrorCode(), e.getMessage());
		} catch (Exception e) {
			// 500 Internal Server Error
			resp.sendError(500);
			e.printStackTrace();
		}

		resp.setStatus(200);
	}

	@Override
	protected void doDelete(HttpServletRequest req, HttpServletResponse resp)
			throws ServletException, IOException {
		// prepare
		ServletHelperMethods.logRequest(logger, req);

		try {
			// get query parameters (returns 400)
			KeygroupID keygroupID = ServletHelperMethods.parseParameter(req, new KeygroupID());
			ClientID clientID = ServletHelperMethods.parseParameter(req, new ClientID());

			// gather configs (returns 404)
			ClientConfig clientConfig = ServletHelperMethods.getConfig(clientID, fBase);

			// parse and verify body (returns 400, 401)
			ServletHelperMethods.parseAndVerifyBody(req, clientConfig);

			// ask naming service to delete
			fBase.namingServiceSender.sendKeygroupConfigDelete(keygroupID);
		} catch (FBaseRestException e) {
			logger.warn(e.getMessage(), e);
			resp.sendError(e.getHttpErrorCode(), e.getMessage());
		} catch (Exception e) {
			logger.warn(e);
			resp.sendError(500);
		}

		resp.setStatus(200);
	}

}
