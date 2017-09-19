package de.hasenburg.fbase.rest.jersey;

import javax.inject.Inject;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
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
import model.config.KeygroupConfig;
import model.config.NodeConfig;
import model.config.ReplicaNodeConfig;
import model.config.TriggerNodeConfig;
import model.data.ClientID;
import model.data.KeygroupID;
import model.messages.Message;

/**
 * 
 * The supported methods are: <br>
 * GET {@link KeygroupConfig} for a given {@link KeygroupID} <br>
 * POST given {@link KeygroupConfig} <br>
 * DELETE {@link KeygroupConfig} with a given {@link KeygroupID} <br>
 * LIST all {@link KeygroupConfig} present at the node <br>
 * 
 * Update Keygroups by <br>
 * * Adding a {@link ClientConfig} <br>
 * * Adding a {@link ReplicaNodeConfig} <br>
 * * Adding a {@link ReplicaNodeConfig} <br>
 * * Adding a {@link TriggerNodeConfig} <br>
 * * Updating the Crypto information <br>
 * * Deleting a {@link ClientConfig} <br>
 * * Deleting a {@link NodeConfig} <br>
 * 
 * 
 * TODO C Methods should only be possible to use for clients that are a part of keygroup
 * (besides LIST and GET)
 * 
 * @author jonathanhasenburg
 *
 */
@Path("keygroups")
public class KeygroupsResource {

	private static Logger logger = Logger.getLogger(KeygroupsResource.class.getName());

	@Inject
	FBase fBase;

	@GET
	@Path("{keygroupID}")
	@Produces(MediaType.APPLICATION_JSON)
	public Response getKeygroupConfig(@PathParam("keygroupID") String keygroupID) {

		KeygroupConfig config = null;
		try {
			config = fBase.configAccessHelper
					.keygroupConfig_get(KeygroupID.createFromString(keygroupID));

			if (config == null) {
				return Response.status(404, "Keygroup does not exist").build();
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

	@DELETE
	@Produces(MediaType.TEXT_PLAIN)
	@Path("{keygroupID}")
	public Response deleteKeygroupConfig(@PathParam("keygroupID") String clientID) {

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
