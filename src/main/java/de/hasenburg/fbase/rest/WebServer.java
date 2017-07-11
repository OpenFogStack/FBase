package de.hasenburg.fbase.rest;

import org.apache.log4j.Logger;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.HandlerList;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;

import control.FBase;

public class WebServer {

	private static Logger logger = Logger.getLogger(WebServer.class.getName());
	
	/**
	 * The jetty server serving all requests.
	 */
	private final Server server;

	public WebServer(FBase fBase) {
		int port = fBase.configuration.getRestPort();
		logger.info("Setting up server at port " + port);

		server = new Server(port);

		// servlet handlers
		ServletContextHandler servletContext = new ServletContextHandler(ServletContextHandler.SESSIONS);
		servletContext.setContextPath("/");
		servletContext.addServlet(new ServletHolder(new RecordServlet(fBase)), "/record");
		servletContext.addServlet(new ServletHolder(new RecordListServlet(fBase)), "/record/list");

		// add handlers to HandlerList
		HandlerList handlers = new HandlerList();
		handlers.addHandler(servletContext);

		// add HanderList to server
		server.setHandler(handlers);
	}

	public void startServer() {
		try {
			server.start();
			logger.info("Server started successfully");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public void stopServer() {
		try {
			server.stop();
			logger.info("Server stopped");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
