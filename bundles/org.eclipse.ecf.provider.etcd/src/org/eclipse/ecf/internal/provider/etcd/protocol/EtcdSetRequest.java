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
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.net.HttpURLConnection;
import java.util.Map;

public class EtcdSetRequest extends EtcdRequest {

	private static final String CONTENT_TYPE_VALUE = "application/x-www-form-urlencoded"; //$NON-NLS-1$
	private static final String CONTENT_TYPE = "Content-Type"; //$NON-NLS-1$

	public EtcdSetRequest(String url, Map<String, String> params) {
		super(url);
	}

	public EtcdSetRequest(String url, String value, int ttl, boolean prevExist) {
		super(url);
		if (value != null)
			setQueryParam(VALUE, value);
		if (ttl > 0)
			setQueryParam(TTL, String.valueOf(ttl));
		if (prevExist)
			setQueryParam(PREVEXIST, String.valueOf(true));
	}

	public EtcdSetRequest(String url, String value, int ttl) {
		this(url, value, ttl, false);
	}

	public EtcdSetRequest(String url, String value) {
		this(url, value, 0);
	}

	public EtcdSetRequest(String directoryURL, int ttl, boolean prevExist) {
		super(directoryURL);
		setQueryParam(DIR, String.valueOf(true));
		if (ttl > 0)
			setQueryParam(TTL, String.valueOf(ttl));
		if (prevExist)
			setQueryParam(PREVEXIST, String.valueOf(true));
	}

	public EtcdSetRequest(String directoryURL, int ttl) {
		this(directoryURL, ttl, false);
	}

	public EtcdSetRequest(String directoryURL) {
		this(directoryURL, 0);
	}

	@Override
	protected HttpURLConnection setConnectionOptions(HttpURLConnection conn)
			throws IOException {
		HttpURLConnection c = super.setConnectionOptions(conn);
		c.setDoOutput(true);
		conn.setRequestProperty(CONTENT_TYPE, CONTENT_TYPE_VALUE);
		return conn;
	}

	protected HttpURLConnection setRequestMethod(HttpURLConnection conn)
			throws IOException {
		conn.setRequestMethod("PUT"); //$NON-NLS-1$
		Map<String, String> params = getQueryParams();
		if (params != null && params.size() > 0) {
			OutputStream os = conn.getOutputStream();
			OutputStreamWriter writer = new OutputStreamWriter(os);
			writer.write(getQueryAsString(params));
			writer.close();
			os.close();
		}
		return conn;
	}

}
