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

public class EtcdGetRequest extends AbstractEtcdRequest {

	private final String url;
	private final Boolean recursive;

	public EtcdGetRequest(String url) {
		super(GET);
		this.url = url;
		this.recursive = null;
	}

	public EtcdGetRequest(String directoryURL, boolean recursive) {
		super(GET);
		this.url = directoryURL;
		this.recursive = new Boolean(recursive);
	}

	@Override
	public AbstractEtcdResponse execute() throws EtcdException {
		HttpURLConnection conn = null;
		try {
			String u = getUrl();
			if (u.endsWith("/")) { //$NON-NLS-1$
				// directory so if recursive is set then we add on
				if (this.recursive != null && this.recursive.booleanValue())
					u = u + "?recursive=true"; //$NON-NLS-1$
			}
			URL url = new URL(u);
			String protocol = url.getProtocol();
			if (!("http".equals(protocol) || "https".equals(protocol)))throw new IOException("url=" + url + " not http protocol"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
			conn = (HttpURLConnection) url.openConnection();
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

	public String getUrl() {
		return url;
	}

	public Boolean getRecursive() {
		return recursive;
	}

	@Override
	public String toString() {
		return "EtcdGetRequest [url=" + url + ", recursive=" + recursive + "]"; //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
	}

}
