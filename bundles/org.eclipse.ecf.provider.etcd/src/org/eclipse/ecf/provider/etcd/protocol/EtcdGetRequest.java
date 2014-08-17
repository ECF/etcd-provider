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
import java.net.HttpURLConnection;

import org.eclipse.ecf.provider.etcd.EtcdException;
import org.json.JSONException;

public class EtcdGetRequest extends EtcdRequest {

	private final Boolean recursive;

	public EtcdGetRequest(String url) {
		super(url);
		this.recursive = null;
	}

	public EtcdGetRequest(String directoryURL, boolean recursive) {
		super(directoryURL);
		this.recursive = new Boolean(recursive);
	}

	@Override
	public EtcdResponse execute() throws EtcdException {
		String u = getUrl();
		if (u.endsWith("/")) { //$NON-NLS-1$
			// directory so if recursive is set then we add on
			if (this.recursive != null && this.recursive.booleanValue())
				this.url = u + "?recursive=true"; //$NON-NLS-1$
		}
		return super.execute();
	}

	public Boolean getRecursive() {
		return recursive;
	}

	@Override
	public String toString() {
		return "EtcdGetRequest [url=" + getUrl() + ", recursive=" + recursive + "]"; //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
	}

	@Override
	protected EtcdResponse doRequest(HttpURLConnection conn)
			throws IOException, JSONException {
		return getResponseOrError(conn);
	}

}
