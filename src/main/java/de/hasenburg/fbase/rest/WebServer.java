package de.hasenburg.fbase.rest;

import java.io.IOException;

import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerRequestFilter;

import org.apache.log4j.Logger;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.HandlerList;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.glassfish.jersey.internal.inject.AbstractBinder;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.servlet.ServletContainer;

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
		ServletContextHandler servletContext =
				new ServletContextHandler(ServletContextHandler.SESSIONS);
		servletContext.setContextPath("/");
		servletContext.addServlet(new ServletHolder(new RecordServlet(fBase)), "/record");
		servletContext.addServlet(new ServletHolder(new RecordListServlet(fBase)), "/record/list");
		servletContext.addServlet(new ServletHolder(new KeygroupConfigServlet(fBase)),
				"/keygroupConfig");
		
		// jersey
		ResourceConfig config = new AppResourceConfig(fBase);
		ServletHolder jersey = new ServletHolder(new ServletContainer(config));
		servletContext.addServlet(jersey, "/jersey/*");

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
	
	/**
	 * Resource config to set jersey resource package and inject FBase object
	 */
	class AppResourceConfig extends ResourceConfig {
	    public AppResourceConfig(FBase fBase) {
	        register(new AppBinder(fBase));
	        register(new ContainerRequestFilter() {
				
				@Override
				public void filter(ContainerRequestContext requestContext) throws IOException {
					logger.info("Received request " + requestContext.getUriInfo());
				}
			});
	        packages("de.hasenburg.fbase.rest.jersey");
	    }
	    
		class AppBinder extends AbstractBinder {
			
			private FBase fBase;
			
		    public AppBinder(FBase fBase) {
				this.fBase = fBase;
			}

			@Override
		    protected void configure() {
		        bind(fBase).to(FBase.class);
		    }
		}
	    
	}
	


}
