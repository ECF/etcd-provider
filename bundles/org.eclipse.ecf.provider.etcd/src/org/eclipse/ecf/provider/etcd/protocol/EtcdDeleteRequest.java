package org.eclipse.ecf.provider.etcd.protocol;

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
