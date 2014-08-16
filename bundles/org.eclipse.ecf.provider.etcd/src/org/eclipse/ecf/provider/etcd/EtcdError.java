package org.eclipse.ecf.provider.etcd;

import java.util.Map;

import org.json.JSONException;
import org.json.JSONObject;

public class EtcdError extends AbstractEtcdResponse {

	public static final String CAUSE_KEY = "cause"; //$NON-NLS-1$
	public static final String ERRORCODE_KEY = "errorCode"; //$NON-NLS-1$
	public static final String INDEX_KEY = "index"; //$NON-NLS-1$
	public static final String MESSAGE_KEY = "message"; //$NON-NLS-1$

	private Map<String, Object> responseHeaders;

	private final String cause;
	private final int errorCode;
	private final int index;
	private final String message;

	public EtcdError(String json, Map<String, Object> headers)
			throws JSONException {
		JSONObject jsonObject = new JSONObject(json);
		this.cause = jsonObject.getString(CAUSE_KEY);
		this.errorCode = jsonObject.getInt(ERRORCODE_KEY);
		this.index = jsonObject.getInt(INDEX_KEY);
		this.message = jsonObject.getString(MESSAGE_KEY);
		this.responseHeaders = headers;
	}

	public Map<String, Object> getResponseHeaders() {
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
		return "EtcdError [responseHeaders=" + responseHeaders + ", cause=" //$NON-NLS-1$ //$NON-NLS-2$
				+ cause + ", errorCode=" + errorCode + ", index=" + index //$NON-NLS-1$ //$NON-NLS-2$
				+ ", message=" + message + "]"; //$NON-NLS-1$ //$NON-NLS-2$
	}

}
