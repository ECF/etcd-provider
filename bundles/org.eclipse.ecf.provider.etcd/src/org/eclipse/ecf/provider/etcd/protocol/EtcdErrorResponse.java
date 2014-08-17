package org.eclipse.ecf.provider.etcd.protocol;

import java.util.List;
import java.util.Map;

import org.json.JSONException;
import org.json.JSONObject;

public class EtcdErrorResponse extends EtcdResponse {

	public static final String CAUSE_KEY = "cause"; //$NON-NLS-1$
	public static final String ERRORCODE_KEY = "errorCode"; //$NON-NLS-1$
	public static final String INDEX_KEY = "index"; //$NON-NLS-1$
	public static final String MESSAGE_KEY = "message"; //$NON-NLS-1$

	private Map<String, List<String>> responseHeaders;

	private final String cause;
	private final int errorCode;
	private final int index;
	private final String message;

	public EtcdErrorResponse(String json, Map<String, List<String>> headers)
			throws JSONException {
		JSONObject jsonObject = new JSONObject(json);
		this.cause = jsonObject.getString(CAUSE_KEY);
		this.errorCode = jsonObject.getInt(ERRORCODE_KEY);
		this.index = jsonObject.getInt(INDEX_KEY);
		this.message = jsonObject.getString(MESSAGE_KEY);
		this.responseHeaders = headers;
	}

	public Map<String, List<String>> getResponseHeaders() {
		return responseHeaders;
	}

	public String getCause() {
		return cause;
	}

	public int getErrorCode() {
		return errorCode;
	}

	public int getIndex() {
		return index;
	}

	public String getMessage() {
		return message;
	}

	@Override
	public String toString() {
		return "EtcdErrorResponse[cause=" //$NON-NLS-1$
				+ cause
				+ ", errorCode=" + errorCode + ", index=" + index //$NON-NLS-1$ //$NON-NLS-2$
				+ ", message=" + message + ", responseHeaders=" + responseHeaders + "]"; //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
	}

	@Override
	public boolean isError() {
		return true;
	}

	@Override
	public EtcdSuccessResponse getResponse() {
		return null;
	}

	@Override
	public EtcdErrorResponse getError() {
		return this;
	}

}
