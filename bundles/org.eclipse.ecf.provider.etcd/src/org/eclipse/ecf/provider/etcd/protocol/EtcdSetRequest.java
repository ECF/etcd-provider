/*******************************************************************************
 * Copyright (c) 2014 Composent, Inc. All rights reserved. This
 * program and the accompanying materials are made available under the terms of
 * the Eclipse Public License v1.0 which accompanies this distribution, and is
 * available at http://www.eclipse.org/legal/epl-v10.html
 * 
 * Contributors: Scott Lewis - initial API and implementation
 ******************************************************************************/
package org.eclipse.ecf.provider.etcd.protocol;

import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.UnsupportedEncodingException;
import java.net.HttpURLConnection;
import java.net.ProtocolException;
import java.util.HashMap;
import java.util.Map;

import org.json.JSONException;

public class EtcdSetRequest extends EtcdRequest {

	private static final String CONTENT_TYPE_VALUE = "application/x-www-form-urlencoded"; //$NON-NLS-1$
	private static final String CONTENT_TYPE = "Content-Type"; //$NON-NLS-1$
	protected Map<String, String> params;

	public EtcdSetRequest(String url, Map<String, String> params) {
		super(url);
		this.params = params;
	}

	public EtcdSetRequest(String url, String value, int ttl, boolean prevExist) {
		super(url);
		Map<String, String> map = new HashMap<String, String>();
		if (value != null)
			map.put(VALUE, value);
		if (ttl > 0)
			map.put(TTL, String.valueOf(ttl));
		if (prevExist)
			map.put(PREVEXIST, String.valueOf(true));
		this.params = map;
	}

	public EtcdSetRequest(String url, String value, int ttl) {
		this(url, value, ttl, false);
	}

	public EtcdSetRequest(String url, String value) {
		this(url, value, 0);
	}

	public Map<String, String> getParams() {
		return params;
	}

	public EtcdSetRequest(String directoryURL, int ttl, boolean prevExist) {
		super(directoryURL);
		Map<String, String> map = new HashMap<String, String>();
		map.put(DIR, String.valueOf(true));
		if (ttl > 0)
			map.put(TTL, String.valueOf(ttl));
		if (prevExist)
			map.put(PREVEXIST, String.valueOf(true));
		this.params = map;
	}

	public EtcdSetRequest(String directoryURL, int ttl) {
		this(directoryURL, ttl, false);
	}

	public EtcdSetRequest(String directoryURL) {
		this(directoryURL, 0);
	}

	private String getQueryAsString(Map<String, String> params)
			throws UnsupportedEncodingException {
		StringBuilder result = new StringBuilder();
		boolean first = true;

		for (String paramName : params.keySet()) {
			if (first)
				first = false;
			else
				result.append("&"); //$NON-NLS-1$

			result.append(paramName);
			result.append("="); //$NON-NLS-1$
			result.append(params.get(paramName));
		}
		return result.toString();
	}

	protected void setRequestMethod(HttpURLConnection conn)
			throws ProtocolException {
		conn.setRequestMethod("PUT"); //$NON-NLS-1$
	}

	@Override
	protected EtcdResponse doRequest(HttpURLConnection conn)
			throws IOException, JSONException {
		conn.setDoOutput(true);
		setRequestMethod(conn);
		conn.setRequestProperty(CONTENT_TYPE, CONTENT_TYPE_VALUE);
		Map<String, String> params = getParams();
		if (params != null && params.size() > 0) {
			OutputStream os = conn.getOutputStream();
			OutputStreamWriter writer = new OutputStreamWriter(os);
			writer.write(getQueryAsString(params));
			writer.close();
			os.close();
		}
		conn.connect();
		return getResponseOrError(conn);
	}

}
