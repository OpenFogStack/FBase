package de.hasenburg.fbase.rest;

import java.io.IOException;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.log4j.Logger;

import control.FBase;
import exceptions.FBaseRestException;
import model.JSONable;
import model.config.ClientConfig;
import model.data.ClientID;
import model.messages.Message;

/**
 * 
 * This servlet offers clients the ability to perform C, D operations for
 * {@link ClientConfig}s.
 * 
 * @author jonathanhasenburg
 *
 */
public class ClientConfigServlet extends HttpServlet {

	private static final long serialVersionUID = 1L;
	private static Logger logger = Logger.getLogger(ClientConfigServlet.class.getName());
	private final FBase fBase;

	public ClientConfigServlet(FBase fBase) {
		this.fBase = fBase;
	}

	@Override
	protected void doPost(HttpServletRequest req, HttpServletResponse resp)
			throws ServletException, IOException {
		// prepare
		ServletHelperMethods.logRequest(logger, req);

		try {
			// parse body (returns 400)
			Message m = ServletHelperMethods.parseBody(req);

			// read in new ClientConfig (returns 400)
			ClientConfig config = JSONable.fromJSON(m.getContent(), ClientConfig.class);
			if (config == null) {
				throw new FBaseRestException(FBaseRestException.BODY_NOT_PARSEABLE, 400);
			}

			// create at namingservice and store 
			if (!fBase.namingServiceSender.sendClientConfigCreate(config)) {
				throw new FBaseRestException(FBaseRestException.CLIENT_ALREADY_EXISTS, 412);
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
			ClientID clientID = ServletHelperMethods.parseParameter(req, new ClientID());
			
			// ask naming service to delete
			if (!fBase.namingServiceSender.sendClientConfigDelete(clientID)) {
				throw new FBaseRestException(FBaseRestException.NOT_FOUND_CLIENT, 404);
			}
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
