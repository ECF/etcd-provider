/*******************************************************************************
 * Copyright (c) 2014 Composent, Inc. All rights reserved. This
 * program and the accompanying materials are made available under the terms of
 * the Eclipse Public License v1.0 which accompanies this distribution, and is
 * available at http://www.eclipse.org/legal/epl-v10.html
 * 
 * Contributors: Scott Lewis - initial API and implementation
 ******************************************************************************/
package org.eclipse.ecf.provider.etcd;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;

import org.json.JSONException;

public abstract class AbstractEtcdRequest extends AbstractEtcdProtocol {

	public static final int GET = 0x00;
	public static final int PUT = 0x10;
	public static final int DELETE = 0x100;

	public static final String VALUE = "value"; //$NON-NLS-1$
	public static final String DIR = "dir"; //$NON-NLS-1$
	public static final String TTL = "ttl"; //$NON-NLS-1$
	public static final String PREVEXIST = "prevExist"; //$NON-NLS-1$
	public static final String RECURSIVE = "recursive"; //$NON-NLS-1$

	protected String url;

	public AbstractEtcdRequest(String url) {
		this.url = url;
	}

	public String getUrl() {
		return url;
	}

	protected AbstractEtcdResponse getResponseOrError(HttpURLConnection conn)
			throws IOException, JSONException {
		try {
			return new EtcdResponse(readStream(conn.getInputStream()),
					conn.getHeaderFields());
		} catch (IOException e) {
			return new EtcdError(readStream(conn.getErrorStream()),
					conn.getHeaderFields());
		}
	}

	protected abstract AbstractEtcdResponse doRequest(HttpURLConnection conn)
			throws IOException, JSONException;

	public AbstractEtcdResponse execute() throws EtcdException {
		HttpURLConnection conn = null;
		try {
			URL url = new URL(getUrl());
			String protocol = url.getProtocol();
			if (!("http".equals(protocol) || "https".equals(protocol)))throw new IOException("url=" + url + " not http protocol"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
			conn = (HttpURLConnection) url.openConnection();
			conn.setReadTimeout(READ_TIMEOUT);
			conn.setConnectTimeout(CONNECT_TIMEOUT);
			return doRequest(conn);
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
