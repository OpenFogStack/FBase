package de.hasenburg.fbase.model;

import crypto.CryptoProvider.EncryptionAlgorithm;
import model.JSONable;

public class CryptoWrapper implements JSONable {

	String encryptionSecret = null;
	EncryptionAlgorithm encryptionAlgorithm = null;

	public CryptoWrapper() {

	}

	public CryptoWrapper(String encryptionSecret, EncryptionAlgorithm encryptionAlgorithm) {
		this.encryptionSecret = encryptionSecret;
		this.encryptionAlgorithm = encryptionAlgorithm;
	}

	public String getEncryptionSecret() {
		return encryptionSecret;
	}

	public void setEncryptionSecret(String encryptionSecret) {
		this.encryptionSecret = encryptionSecret;
	}

	public EncryptionAlgorithm getEncryptionAlgorithm() {
		return encryptionAlgorithm;
	}

	public void setEncryptionAlgorithm(EncryptionAlgorithm encryptionAlgorithm) {
		this.encryptionAlgorithm = encryptionAlgorithm;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result
				+ ((encryptionAlgorithm == null) ? 0 : encryptionAlgorithm.hashCode());
		result = prime * result + ((encryptionSecret == null) ? 0 : encryptionSecret.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		CryptoWrapper other = (CryptoWrapper) obj;
		if (encryptionAlgorithm != other.encryptionAlgorithm)
			return false;
		if (encryptionSecret == null) {
			if (other.encryptionSecret != null)
				return false;
		} else if (!encryptionSecret.equals(other.encryptionSecret))
			return false;
		return true;
	}

}
