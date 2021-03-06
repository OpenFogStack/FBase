package de.hasenburg.fbase.rest.jersey;

import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import javax.inject.Inject;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.apache.log4j.Logger;

import control.FBase;
import exceptions.FBaseStorageConnectorException;
import model.JSONable;
import model.data.DataIdentifier;
import model.data.DataRecord;
import model.data.KeygroupID;
import model.messages.Message;

/**
 * 
 * The supported methods are: <br>
 * GET {@link DataRecord} for a given {@link DataIdentifier} <br>
 * PUT given {@link DataRecord} <br>
 * DELETE {@link DataRecord} with a given {@link DataIdentifier} <br>
 * LIST all {@link DataIdentifier} for a given {@link KeygroupID}
 * 
 * TODO C Methods should only be possible to use for clients that are a part of keygroup
 * 
 * @author jonathanhasenburg
 *
 */
@Path("records")
public class RecordsResource {

	private static Logger logger = Logger.getLogger(RecordsResource.class.getName());

	@Inject
	FBase fBase;

	@GET
	@Path("{app}/{tenant}/{group}")
	@Produces(MediaType.APPLICATION_JSON)
	public Response listDataRecords(@PathParam("app") String app,
			@PathParam("tenant") String tenant, @PathParam("group") String group) {

		Set<DataIdentifier> dataIdentifiers = null;
		try {
			dataIdentifiers = fBase.connector.dataRecords_list(new KeygroupID(app, tenant, group));

			if (dataIdentifiers == null) {
				return Response.status(404, "Keygroup does not exist").build();
			}
		} catch (FBaseStorageConnectorException e) {
			logger.warn(e);
			return Response.status(500, e.getMessage()).build();
		}

		Message m = new Message();
		String json = dataIdentifiers.stream().map(e -> JSONable.toJSON(e))
				.collect(Collectors.joining(", "));
		m.setContent("[" + json + "]");
		return Response.ok(JSONable.toJSON(m)).build();
	}

	@GET
	@Path("{app}/{tenant}/{group}/{dataID}")
	@Produces(MediaType.APPLICATION_JSON)
	public Response getDataRecord(@PathParam("app") String app, @PathParam("tenant") String tenant,
			@PathParam("group") String group, @PathParam("dataID") String dataID) {

		DataRecord record = null;
		try {
			record = fBase.connector.dataRecords_get(
					new DataIdentifier(new KeygroupID(app, tenant, group), dataID));

			if (record == null) {
				return Response.status(404, "Record does not exist").build();
			}
		} catch (FBaseStorageConnectorException e) {
			logger.warn(e);
			return Response.status(500, e.getMessage()).build();
		}

		Message m = new Message();
		m.setContent(JSONable.toJSON(record));
		return Response.ok(JSONable.toJSON(m)).build();
	}

	@PUT
	@Produces(MediaType.TEXT_PLAIN)
	@Consumes(MediaType.APPLICATION_JSON)
	public Response putDataRecord(String json) {

		DataRecord record = JSONable.fromJSON(json, DataRecord.class);
		if (record == null) {
			return Response.status(400, "Body is not a data record").build();
		}

		Future<Boolean> future = fBase.taskmanager.runPutDataRecordTask(record, true);
		try {
			future.get(5, TimeUnit.SECONDS);
		} catch (ExecutionException e) {
			logger.error(e.getCause());
			return Response.status(500, e.getCause().getMessage()).build();
		} catch (TimeoutException | InterruptedException e) {
			logger.error(e);
		}

		return Response.ok().build();
	}

	@DELETE
	@Path("{app}/{tenant}/{group}/{dataID}")
	@Produces(MediaType.TEXT_PLAIN)
	public Response deleteDataRecord(@PathParam("app") String app,
			@PathParam("tenant") String tenant, @PathParam("group") String group,
			@PathParam("dataID") String dataID) {

		Future<Boolean> future = fBase.taskmanager.runDeleteDataRecordTask(
				new DataIdentifier(new KeygroupID(app, tenant, group), dataID), true);

		try {
			future.get(5, TimeUnit.SECONDS);
		} catch (ExecutionException e) {
			logger.error(e.getCause());
			return Response.status(500, e.getCause().getMessage()).build();
		} catch (TimeoutException | InterruptedException e) {
			logger.error(e);
		}

		return Response.ok().build();
	}

}
