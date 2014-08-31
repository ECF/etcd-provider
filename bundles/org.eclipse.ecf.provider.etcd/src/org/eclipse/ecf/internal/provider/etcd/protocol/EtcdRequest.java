/*******************************************************************************
 * Copyright (c) 2014 Composent, Inc. All rights reserved. This
 * program and the accompanying materials are made available under the terms of
 * the Eclipse Public License v1.0 which accompanies this distribution, and is
 * available at http://www.eclipse.org/legal/epl-v10.html
 * 
 * Contributors: Scott Lewis - initial API and implementation
 ******************************************************************************/
package org.eclipse.ecf.internal.provider.etcd.protocol;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.ProtocolException;
import java.net.URL;
import java.net.URLEncoder;
import java.util.HashMap;
import java.util.Map;

import org.eclipse.ecf.internal.provider.etcd.DebugOptions;
import org.eclipse.ecf.internal.provider.etcd.LogUtility;
import org.json.JSONException;

public abstract class EtcdRequest extends EtcdProtocol {

	public static final int GET = 0x00;
	public static final int PUT = 0x10;
	public static final int DELETE = 0x100;

	public static final String VALUE = "value"; //$NON-NLS-1$
	public static final String DIR = "dir"; //$NON-NLS-1$
	public static final String TTL = "ttl"; //$NON-NLS-1$
	public static final String PREVEXIST = "prevExist"; //$NON-NLS-1$
	public static final String RECURSIVE = "recursive"; //$NON-NLS-1$
	public static final String WAIT = "wait"; //$NON-NLS-1$
	public static final String WAITINDEX = "waitIndex"; //$NON-NLS-1$

	private final String url;
	private final Map<String, String> queryParams;

	public EtcdRequest(String url) {
		this.url = url;
		queryParams = new HashMap<String, String>();
	}

	public String getUrl() {
		return url;
	}

	public Map<String, String> getQueryParams() {
		return queryParams;
	}

	public void setQueryParam(String name, String value) {
		getQueryParams().put(name, value);
	}

	public void setQueryBoolean(String name) {
		setQueryParam(name, String.valueOf(true));
	}

	protected EtcdResponse getResponseOrError(HttpURLConnection conn)
			throws IOException, JSONException {
		try {
			return new EtcdSuccessResponse(readStream(conn.getInputStream()),
					conn.getHeaderFields());
		} catch (IOException e) {
			return new EtcdErrorResponse(readStream(conn.getErrorStream()),
					conn.getHeaderFields());
		}
	}

	protected HttpURLConnection setRequestMethod(HttpURLConnection conn)
			throws ProtocolException, IOException {
		// by default do nothing and default is GET
		return conn;
	}

	protected String getQueryAsString(Map<String, String> params) {
		StringBuffer buf = new StringBuffer();
		int queryParamCount = 0;
		for (String qkey : queryParams.keySet()) {
			if (queryParamCount++ == 0)
				buf.append('?');
			else
				buf.append('&');
			try {
				buf.append(URLEncoder.encode(qkey, "UTF-8")).append('=').append(URLEncoder.encode(queryParams.get(qkey), "UTF-8")); //$NON-NLS-1$ //$NON-NLS-2$
			} catch (UnsupportedEncodingException e) {
				// should never happen
			}
		}
		return buf.toString();
	}

	protected String getUrlWithQuery() {
		return new StringBuffer(getUrl()).append(
				getQueryAsString(getQueryParams())).toString();
	}

	protected HttpURLConnection setConnectionOptions(HttpURLConnection conn)
			throws IOException {
		conn.setReadTimeout(READ_TIMEOUT);
		conn.setConnectTimeout(CONNECT_TIMEOUT);
		return conn;
	}

	public EtcdResponse execute() throws EtcdException {
		HttpURLConnection conn = null;
		try {
			// Create url (with any query parameters)
			URL url = new URL(getUrlWithQuery());
			LogUtility
					.trace("execute", DebugOptions.PROTOCOL, this.getClass(), "url=" + url); //$NON-NLS-1$//$NON-NLS-2$
			String protocol = url.getProtocol();
			if (!("http".equals(protocol) || "https".equals(protocol)))throw new IOException("url=" + url + " not http protocol"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
			conn = (HttpURLConnection) url.openConnection();
			setConnectionOptions(conn);
			setRequestMethod(conn);
			return getResponseOrError(conn);
		} catch (MalformedURLException e) {
			throw new EtcdException("Url " + getUrl() + " is malformed", e); //$NON-NLS-1$ //$NON-NLS-2$
		} catch (IOException e) {
			throw new EtcdException("Error communicating with server", e); //$NON-NLS-1$
		} catch (JSONException e) {
			throw new EtcdException("Parsing error", e); //$NON-NLS-1$
		} finally {
			if (conn != null)
				conn.disconnect();
		}
	}

}
