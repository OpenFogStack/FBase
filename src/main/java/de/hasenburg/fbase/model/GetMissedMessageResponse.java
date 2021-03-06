package de.hasenburg.fbase.model;

import model.JSONable;
import model.data.DataIdentifier;
import model.data.DataRecord;

public class GetMissedMessageResponse implements JSONable {

	private DataIdentifier dataIdentifier = null;
	private DataRecord dataRecord = null;
	private String textualInfo = null;

	public GetMissedMessageResponse() {

	}

	public GetMissedMessageResponse(DataIdentifier dataIdentifier, DataRecord dataRecord) {
		super();
		this.dataIdentifier = dataIdentifier;
		this.dataRecord = dataRecord;
	}

	public DataIdentifier getDataIdentifier() {
		return dataIdentifier;
	}

	public void setDataIdentifier(DataIdentifier dataIdentifier) {
		this.dataIdentifier = dataIdentifier;
	}

	public DataRecord getDataRecord() {
		return dataRecord;
	}

	public void setDataRecord(DataRecord dataRecord) {
		this.dataRecord = dataRecord;
	}

	public String getTextualInfo() {
		return textualInfo;
	}

	public void setTextualInfo(String textualInfo) {
		this.textualInfo = textualInfo;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((dataIdentifier == null) ? 0 : dataIdentifier.hashCode());
		result = prime * result + ((dataRecord == null) ? 0 : dataRecord.hashCode());
		result = prime * result + ((textualInfo == null) ? 0 : textualInfo.hashCode());
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
		GetMissedMessageResponse other = (GetMissedMessageResponse) obj;
		if (dataIdentifier == null) {
			if (other.dataIdentifier != null)
				return false;
		} else if (!dataIdentifier.equals(other.dataIdentifier))
			return false;
		if (dataRecord == null) {
			if (other.dataRecord != null)
				return false;
		} else if (!dataRecord.equals(other.dataRecord))
			return false;
		if (textualInfo == null) {
			if (other.textualInfo != null)
				return false;
		} else if (!textualInfo.equals(other.textualInfo))
			return false;
		return true;
	}

}
