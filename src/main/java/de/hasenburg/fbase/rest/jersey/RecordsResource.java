package de.hasenburg.fbase.rest.jersey;

import javax.inject.Inject;
import javax.ws.rs.GET;
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
import model.messages.Message;

@Path("records")
public class RecordsResource {
	
	private static Logger logger = Logger.getLogger(RecordsResource.class.getName());

	@Inject
	FBase fBase;
	
	@GET
	@Path("{dataIdentifier}")
	@Produces(MediaType.APPLICATION_JSON)
	public Response getNodeConfig(@PathParam("dataIdentifier") String dataIdentifer) {
		
		DataRecord record = null;
		try {
			record = fBase.connector.dataRecords_get(DataIdentifier.createFromString(dataIdentifer));

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
	
}
