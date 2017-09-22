package de.hasenburg.fbase.rest.jersey;

import javax.inject.Inject;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.apache.log4j.Logger;

import control.FBase;
import exceptions.FBaseCommunicationException;
import exceptions.FBaseNamingServiceException;
import exceptions.FBaseStorageConnectorException;
import model.JSONable;
import model.config.ClientConfig;
import model.data.ClientID;
import model.messages.Message;

/**
 * 
 * The supported methods are: <br>
 * GET {@link ClientConfig} for a given {@link ClientID} <br>
 * POST given {@link ClientConfig} <br>
 * PUT {@link ClientConfig} by replacing current with given (something must exist) <br>
 * DELETE {@link ClientConfig} with a given {@link ClientID}
 * 
 * TODO C <br>
 * PUT should only be possible to own config <br>
 * DELETE should only be possible to own config
 * 
 * @author jonathanhasenburg
 *
 */
@Path("clients")
public class ClientsResource {

	private static Logger logger = Logger.getLogger(ClientsResource.class.getName());

	@Inject
	FBase fBase;

	@GET
	@Path("{clientID}")
	@Produces(MediaType.APPLICATION_JSON)
	public Response getClientConfig(@PathParam("clientID") String clientID) {

		ClientConfig config = null;
		try {
			config = fBase.configAccessHelper.clientConfig_get(new ClientID(clientID));

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
	public Response createClientConfig(String json) {

		ClientConfig clientConfig = JSONable.fromJSON(json, ClientConfig.class);
		if (clientConfig == null) {
			return Response.status(400, "Body is not a client config").build();
		}

		try {
			fBase.namingServiceSender.sendClientConfigCreate(clientConfig);
			fBase.connector.clientConfig_put(clientConfig.getClientID(), clientConfig);
		} catch (FBaseCommunicationException | FBaseNamingServiceException
				| FBaseStorageConnectorException e) {
			logger.warn(e);
			return Response.status(500, e.getMessage()).build();
		}

		return Response.ok().build();
	}

	@PUT
	@Produces(MediaType.TEXT_PLAIN)
	@Consumes(MediaType.APPLICATION_JSON)
	public Response updateClientConfig(String json) {

		ClientConfig clientConfig = JSONable.fromJSON(json, ClientConfig.class);
		if (clientConfig == null) {
			return Response.status(400, "Body is not a client config").build();
		}

		try {
			fBase.namingServiceSender.sendClientConfigUpdate(clientConfig);
			fBase.connector.clientConfig_put(clientConfig.getClientID(), clientConfig);
		} catch (FBaseCommunicationException | FBaseNamingServiceException
				| FBaseStorageConnectorException e) {
			logger.warn(e);
			return Response.status(500, e.getMessage()).build();
		}

		return Response.ok().build();
	}

	@DELETE
	@Produces(MediaType.TEXT_PLAIN)
	@Path("{clientID}")
	public Response deleteClientConfig(@PathParam("clientID") String clientID) {

		try {
			fBase.namingServiceSender.sendClientConfigDelete(new ClientID(clientID));
			// TODO C: Remove config from database
		} catch (FBaseCommunicationException | FBaseNamingServiceException e) {
			logger.warn(e);
			return Response.status(500, e.getMessage()).build();
		}

		return Response.ok().build();
	}

}
