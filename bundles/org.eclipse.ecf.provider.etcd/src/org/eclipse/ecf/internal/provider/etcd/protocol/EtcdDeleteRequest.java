/*******************************************************************************
 * Copyright (c) 2014 Composent, Inc. All rights reserved. This
 * program and the accompanying materials are made available under the terms of
 * the Eclipse Public License v1.0 which accompanies this distribution, and is
 * available at http://www.eclipse.org/legal/epl-v10.html
 * 
 * Contributors: Scott Lewis - initial API and implementation
 ******************************************************************************/
package org.eclipse.ecf.internal.provider.etcd.protocol;

import java.net.HttpURLConnection;
import java.net.ProtocolException;

public class EtcdDeleteRequest extends EtcdSetRequest {

	public EtcdDeleteRequest(String url) {
		super(url, (String) null);
	}

	public EtcdDeleteRequest(String directoryURL, boolean recursive) {
		super(directoryURL);
		if (recursive)
			setQueryBoolean(RECURSIVE);
		else
			setQueryBoolean(DIR);
	}

	protected HttpURLConnection setRequestMethod(HttpURLConnection conn)
			throws ProtocolException {
		conn.setRequestMethod("DELETE"); //$NON-NLS-1$
		return conn;
	}

}
