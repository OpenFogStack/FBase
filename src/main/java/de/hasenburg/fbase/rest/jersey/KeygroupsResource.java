package de.hasenburg.fbase.rest.jersey;

import javax.inject.Inject;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
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
	@Path("{app}/{tenant}/{group}")
	@Produces(MediaType.APPLICATION_JSON)
	public Response getKeygroupConfig(@PathParam("app") String app,
	@PathParam("tenant") String tenant, @PathParam("group") String group) {

		KeygroupConfig config = null;
		try {
			config = fBase.configAccessHelper
					.keygroupConfig_get(new KeygroupID(app, tenant, group));

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
	@Path("{app}/{tenant}/{group}")
	public Response deleteKeygroupConfig(@PathParam("app") String app,
			@PathParam("tenant") String tenant, @PathParam("group") String group) {

		try {
			KeygroupID kID = new KeygroupID(app, tenant, group);
			fBase.namingServiceSender.sendKeygroupConfigDelete(kID);
			// we only get here, if naming service approves

			// get tompstoned version
			KeygroupConfig config = fBase.namingServiceSender.sendKeygroupConfigRead(kID);
			fBase.taskmanager.runUpdateKeygroupConfigTask(config, true); // forward to other
																			// nodes
		} catch (FBaseCommunicationException | FBaseNamingServiceException e) {
			logger.warn(e);
			return Response.status(500, e.getMessage()).build();
		}

		return Response.ok().build();
	}

	@POST
	@Produces(MediaType.TEXT_PLAIN)
	@Consumes(MediaType.APPLICATION_JSON)
	public Response createKeygroupConfig(String json) {

		KeygroupConfig keygroupConfig = JSONable.fromJSON(json, KeygroupConfig.class);
		if (keygroupConfig == null) {
			return Response.status(400, "Body is not a keygroup config").build();
		}

		try {
			KeygroupConfig approvedConfig =
					fBase.namingServiceSender.sendKeygroupConfigCreate(keygroupConfig);
			fBase.taskmanager.runUpdateKeygroupConfigTask(approvedConfig, true);
		} catch (FBaseCommunicationException | FBaseNamingServiceException e) {
			logger.warn(e);
			return Response.status(500, e.getMessage()).build();
		}

		return Response.ok().build();
	}

}
