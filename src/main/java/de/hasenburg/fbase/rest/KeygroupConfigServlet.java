package de.hasenburg.fbase.rest;

import java.io.IOException;
import java.io.PrintWriter;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.log4j.Logger;

import control.FBase;
import exceptions.FBaseRestException;
import model.JSONable;
import model.config.ClientConfig;
import model.config.KeygroupConfig;
import model.data.ClientID;
import model.data.KeygroupID;

/**
 * 
 * This servlet offers clients the ability to perform CRUD operations for
 * {@link KeygroupConfig}s.
 * 
 * @author jonathanhasenburg
 *
 */
public class KeygroupConfigServlet extends HttpServlet {

	private static final long serialVersionUID = 1L;
	private static Logger logger = Logger.getLogger(RecordServlet.class.getName());
	private final FBase fBase;

	public KeygroupConfigServlet(FBase fBase) {
		this.fBase = fBase;
	}

	@Override
	protected void doGet(HttpServletRequest req, HttpServletResponse resp)
			throws ServletException, IOException {

		try {
			// prepare
			ServletHelperMethods.logRequest(logger, req);
			PrintWriter w = resp.getWriter();

			// get query parameters (returns 400)
			KeygroupID keygroupID =
					ServletHelperMethods.parseParameter(req, new KeygroupID());
			ClientID clientID = ServletHelperMethods.parseParameter(req, new ClientID());

			// authorize request (returns 404)
			@SuppressWarnings("unused")
			ClientConfig clientConfig = ServletHelperMethods.getConfig(clientID, fBase);

			// gather requested data (returns 401 and 404)
			KeygroupConfig keygroupConfig =
					ServletHelperMethods.getConfig(keygroupID, fBase);
			if (!keygroupConfig.getClients().contains(clientID)) {
				throw new FBaseRestException(FBaseRestException.NOT_AUTHORIZED, 401);
			}

			// answer (returns 200)
			resp.setStatus(200);
			w.write(JSONable.toJSON(keygroupConfig)); //encrypt using clientConfig data if required
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
