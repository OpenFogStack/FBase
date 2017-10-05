package de.hasenburg.fbase.rest.jersey;

import javax.inject.Inject;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.apache.log4j.Logger;

import control.FBase;
import exceptions.FBaseCommunicationException;
import exceptions.FBaseNamingServiceException;
import exceptions.FBaseStorageConnectorException;
import model.JSONable;
import model.config.NodeConfig;
import model.messages.Message;

/**
 * 
 * TODO C: add update and delete
 * 
 * @author jonathanhasenburg
 *
 */
@Path("nodes")
public class NodesResource {

	private static Logger logger = Logger.getLogger(NodesResource.class.getName());

	@Inject
	FBase fBase;

	@GET
	@Produces(MediaType.APPLICATION_JSON)
	public Response getNodeConfig() {

		NodeConfig config = null;
		try {
			config = fBase.configAccessHelper.nodeConfig_get(fBase.configuration.getNodeID());

			if (config == null) {
				return Response.status(404, "Config does not exist").build();
			}
		} catch (FBaseStorageConnectorException | FBaseCommunicationException
				| FBaseNamingServiceException e) {
			logger.warn(e);
			return Response.status(500, e.getMessage()).build();
		}

		Message m = new Message();
		m.setContent(JSONable.toJSON(config));
		return Response.ok(JSONable.toJSON(m)).build();
	}

	@POST
	@Produces(MediaType.TEXT_PLAIN)
	@Consumes(MediaType.APPLICATION_JSON)
	public Response createNodeConfig(String json) {

		NodeConfig nodeConfig = JSONable.fromJSON(json, NodeConfig.class);
		if (nodeConfig == null) {
			return Response.status(400, "Body is not a node config").build();
		}

		try {
			fBase.namingServiceSender.sendNodeConfigCreate(nodeConfig);
		} catch (FBaseCommunicationException | FBaseNamingServiceException e) {
			logger.warn(e);
			return Response.status(500, e.getMessage()).build();
		}

		return Response.ok().build();
	}

}
