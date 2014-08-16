/*******************************************************************************
 * Copyright (c) 2014 Composent, Inc. All rights reserved. This
 * program and the accompanying materials are made available under the terms of
 * the Eclipse Public License v1.0 which accompanies this distribution, and is
 * available at http://www.eclipse.org/legal/epl-v10.html
 * 
 * Contributors: Scott Lewis - initial API and implementation
 ******************************************************************************/
package org.eclipse.ecf.provider.etcd;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;

import org.json.JSONException;

public abstract class AbstractEtcdProtocol {

	public static final String ACTION_KEY = "action"; //$NON-NLS-1$
	public static final String NODE_KEY = "node"; //$NON-NLS-1$
	public static final String PREVIOUSNODE_KEY = "prevNode"; //$NON-NLS-1$

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

	protected String readStream(InputStream ins) throws IOException {
		try {
			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			byte[] buffer = new byte[1024];
			int length = 0;
			while ((length = ins.read(buffer)) != -1)
				baos.write(buffer, 0, length);
			return new String(baos.toByteArray());
		} finally {
			try {
				ins.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

}
