package org.eclipse.ecf.provider.etcd;

import java.net.HttpURLConnection;
import java.net.ProtocolException;

public class EtcdDeleteRequest extends EtcdSetRequest {

	public EtcdDeleteRequest(String url) {
		super(url, (String) null);
	}

	public EtcdDeleteRequest(String directoryURL, boolean recursive) {
		super(directoryURL);
		if (recursive)
			this.url = this.url + "?" + RECURSIVE + "=true"; //$NON-NLS-1$ //$NON-NLS-2$
		else
			this.url = this.url + "?" + DIR + "=true"; //$NON-NLS-1$ //$NON-NLS-2$
		if (this.params != null)
			this.params.clear();
	}

	protected void setRequestMethod(HttpURLConnection conn)
			throws ProtocolException {
		conn.setRequestMethod("DELETE"); //$NON-NLS-1$
	}

}
