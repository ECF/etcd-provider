/*******************************************************************************
 * Copyright (c) 2014 Composent, Inc. All rights reserved. This
 * program and the accompanying materials are made available under the terms of
 * the Eclipse Public License v1.0 which accompanies this distribution, and is
 * available at http://www.eclipse.org/legal/epl-v10.html
 * 
 * Contributors: Scott Lewis - initial API and implementation
 ******************************************************************************/
package org.eclipse.ecf.internal.provider.etcd.protocol;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;

import org.eclipse.ecf.internal.provider.etcd.DebugOptions;
import org.eclipse.ecf.internal.provider.etcd.LogUtility;

public abstract class EtcdProtocol {

	public static final int READ_TIMEOUT = Integer.parseInt(System.getProperty(
			"org.eclipse.ecf.provider.etcd.readtimeout", "10000")); //$NON-NLS-1$ //$NON-NLS-2$
	public static final int CONNECT_TIMEOUT = Integer.parseInt(System
			.getProperty(
					"org.eclipse.ecf.provider.etcd.connecttimeout", "15000")); //$NON-NLS-1$ //$NON-NLS-2$

	public static final String ACTION_KEY = "action"; //$NON-NLS-1$
	public static final String NODE_KEY = "node"; //$NON-NLS-1$
	public static final String PREVIOUSNODE_KEY = "prevNode"; //$NON-NLS-1$

	public static final String ACTION_SET = "set"; //$NON-NLS-1$
	public static final String ACTION_UPDATE = "update"; //$NON-NLS-1$
	public static final String ACTION_DELETE = "delete"; //$NON-NLS-1$
	public static final String ACTION_EXPIRE = "expire"; //$NON-NLS-1$
	public static final String ACTION_GET = "get"; //$NON-NLS-1$
	public static final String ACTION_CREATE = "create"; //$NON-NLS-1$
	public static final String ACTION_COMPARE_AND_SWAP = "compareAndSwap"; //$NON-NLS-1$
	public static final String ACTION_COMPARE_AND_DELETE = "compareAndDelete"; //$NON-NLS-1$

	protected String readStream(InputStream ins) throws IOException {
		if (ins == null) return "InputStream is null"; //$NON-NLS-1$
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
				LogUtility
						.logError(
								"readStream", DebugOptions.PROTOCOL, getClass(), "Exception closing input stream", e); //$NON-NLS-1$ //$NON-NLS-2$
			}
		}
	}

}
