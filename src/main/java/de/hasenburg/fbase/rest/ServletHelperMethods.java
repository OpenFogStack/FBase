package de.hasenburg.fbase.rest;

import java.io.IOException;
import java.util.stream.Collectors;

import javax.servlet.http.HttpServletRequest;

import org.apache.log4j.Logger;

import control.FBase;
import crypto.CryptoProvider;
import crypto.CryptoProvider.EncryptionAlgorithm;
import exceptions.FBaseRestException;
import exceptions.FBaseStorageConnectorException;
import model.JSONable;
import model.config.ClientConfig;
import model.config.KeygroupConfig;
import model.data.ClientID;
import model.data.DataIdentifier;
import model.data.KeygroupID;

public class ServletHelperMethods {

	public static void logRequest(Logger logger, HttpServletRequest request) {
		logger.debug("Received " + request.getMethod() + "-Request with queryString "
				+ request.getQueryString());
	}

	/*
	 * Parameter Parsing
	 */

	public static DataIdentifier parseParameter(HttpServletRequest request, DataIdentifier field)
			throws FBaseRestException {
		field = DataIdentifier.createFromString(request.getParameter("dataIdentifier"));
		if (field == null) {
			throw new FBaseRestException(FBaseRestException.DATAIDENTIFIER_MISSING, 400);
		}
		return field;
	}

	public static KeygroupID parseParameter(HttpServletRequest request, KeygroupID field)
			throws FBaseRestException {
		field = KeygroupID.createFromString(request.getParameter("keygroupID"));
		if (field == null) {
			throw new FBaseRestException(FBaseRestException.KEYGROUPID_MISSING, 400);
		}
		return field;
	}

	public static ClientID parseParameter(HttpServletRequest request, ClientID field)
			throws FBaseRestException {
		field = new ClientID(request.getParameter("clientID"));
		if (field.getClientID() == null) {
			throw new FBaseRestException(FBaseRestException.CLIENTID_MISSING, 400);
		}
		return field;
	}

	/*
	 * Body Access Authorization
	 */

	public static void authorizeAccessBasedOnBody(HttpServletRequest request, ClientID expected,
			String secret, EncryptionAlgorithm algorithm) throws IOException, FBaseRestException {
		String body =
				request.getReader().lines().collect(Collectors.joining(System.lineSeparator()));
		if (secret != null && algorithm != null) {
			body = CryptoProvider.decrypt(body, secret, algorithm);
		}
		ClientID actually = new ClientID(body);
		if (!expected.equals(actually)) {
			throw new FBaseRestException(FBaseRestException.NOT_AUTHORIZED, 401);
		}
	}

	/*
	 * Body Parsing
	 */

	public static KeygroupConfig parseBodyToConfig(HttpServletRequest req,
			KeygroupConfig keygroupConfig, String secret, EncryptionAlgorithm algorithm)
			throws IOException, FBaseRestException {
		String body = req.getReader().lines().collect(Collectors.joining(System.lineSeparator()));
		if (secret != null && algorithm != null) {
			body = CryptoProvider.decrypt(body, secret, algorithm);
		}
		keygroupConfig = JSONable.fromJSON(body, KeygroupConfig.class);
		if (keygroupConfig == null) {
			throw new FBaseRestException(FBaseRestException.BODY_NOT_PARSEABLE, 400);
		}
		return keygroupConfig;
	}

	/*
	 * Config gathering
	 */

	public static ClientConfig getConfig(ClientID clientID, FBase fBase)
			throws FBaseStorageConnectorException, FBaseRestException {
		ClientConfig clientConfig = fBase.configAccessHelper.clientConfig_get(clientID);
		if (clientConfig == null) {
			throw new FBaseRestException(FBaseRestException.NOT_FOUND_CONFIG, 404);
		}
		return clientConfig;
	}

	public static KeygroupConfig getConfig(KeygroupID keygroupID, FBase fBase)
			throws FBaseStorageConnectorException, FBaseRestException {
		KeygroupConfig keygroupConfig =
				fBase.configAccessHelper.keygroupConfig_get(keygroupID).getValue0();
		if (keygroupConfig == null) {
			throw new FBaseRestException(FBaseRestException.NOT_FOUND_CONFIG, 404);
		}
		return keygroupConfig;
	}

}
