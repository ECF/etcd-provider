/*******************************************************************************
 * Copyright (c) 2014 Composent, Inc. All rights reserved. This
 * program and the accompanying materials are made available under the terms of
 * the Eclipse Public License v1.0 which accompanies this distribution, and is
 * available at http://www.eclipse.org/legal/epl-v10.html
 * 
 * Contributors: Scott Lewis - initial API and implementation
 ******************************************************************************/
package org.eclipse.ecf.provider.etcd;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;

import org.eclipse.ecf.core.util.Base64;
import org.eclipse.ecf.discovery.IServiceInfo;
import org.eclipse.ecf.discovery.IServiceProperties;
import org.eclipse.ecf.discovery.ServiceInfo;
import org.eclipse.ecf.discovery.ServiceProperties;
import org.eclipse.ecf.discovery.identity.IServiceTypeID;
import org.eclipse.ecf.discovery.identity.ServiceIDFactory;
import org.eclipse.ecf.provider.etcd.identity.EtcdNamespace;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.json.JSONStringer;
import org.json.JSONWriter;

public class EtcdServiceInfo extends ServiceInfo {

	private static final long serialVersionUID = -1975271151544601442L;

	public static final String LOCATION_KEY = "location"; //$NON-NLS-1$
	public static final String PRIORITY_KEY = "priority"; //$NON-NLS-1$

	public static final String WEIGHT_KEY = "weight"; //$NON-NLS-1$
	public static final String SERVICENAME_KEY = "servicename"; //$NON-NLS-1$

	public static final String TTL_KEY = "ttl"; //$NON-NLS-1$

	public static final String BYTES_TYPE = "bytes"; //$NON-NLS-1$
	public static final String STRING_TYPE = "string"; //$NON-NLS-1$
	public static final String OTHER_TYPE = "object"; //$NON-NLS-1$
	public static final String TYPE_KEY = "type"; //$NON-NLS-1$
	public static final String NAME_KEY = "name"; //$NON-NLS-1$
	public static final String VALUE_KEY = "value"; //$NON-NLS-1$

	public static final String PROPERTIES_KEY = "properties"; //$NON-NLS-1$

	public static final String SERVICETYPE_KEY = "servicetype"; //$NON-NLS-1$
	public static final String SERVICETYPE_SERVICES_KEY = "services"; //$NON-NLS-1$	
	public static final String SERVICETYPE_SCOPES_KEY = "scopes"; //$NON-NLS-1$
	public static final String SERVICETYPE_PROTOCOLS_KEY = "protocols"; //$NON-NLS-1$
	private static final String SERVICETYPE_NA_KEY = "namingauth"; //$NON-NLS-1$

	public static EtcdServiceInfo deserializeFromString(String jsonString)
			throws JSONException {
		JSONObject jsonObject = new JSONObject(jsonString);
		String locationString = jsonObject.getString(LOCATION_KEY);
		URI location = null;
		try {
			location = new URI(locationString);
		} catch (URISyntaxException e) {
			throw new JSONException(e);
		}
		int priority = jsonObject.getInt(PRIORITY_KEY);
		int weight = jsonObject.getInt(WEIGHT_KEY);
		String serviceName = jsonObject.optString(SERVICENAME_KEY);
		long ttl = jsonObject.getLong(TTL_KEY);

		JSONObject sto = jsonObject.getJSONObject(SERVICETYPE_KEY);
		List<String> l = new ArrayList<String>();
		// services
		JSONArray sa = sto.getJSONArray(SERVICETYPE_SERVICES_KEY);
		for (int i = 0; i < sa.length(); i++)
			l.add(sa.getString(i));
		String[] services = l.toArray(new String[l.size()]);
		l.clear();
		// scopes
		sa = sto.getJSONArray(SERVICETYPE_SCOPES_KEY);
		for (int i = 0; i < sa.length(); i++)
			l.add(sa.getString(i));
		String[] scopes = l.toArray(new String[l.size()]);
		l.clear();
		// protocols
		sa = sto.getJSONArray(SERVICETYPE_PROTOCOLS_KEY);
		for (int i = 0; i < sa.length(); i++)
			l.add(sa.getString(i));
		String[] protocols = l.toArray(new String[l.size()]);
		l.clear();
		// naming auth
		String namingAuth = sto.getString(SERVICETYPE_NA_KEY);

		// Create service type via factory
		IServiceTypeID serviceTypeID = ServiceIDFactory.getDefault()
				.createServiceTypeID(EtcdNamespace.INSTANCE, services, scopes,
						protocols, namingAuth);

		// Service Properties
		IServiceProperties sProps = new ServiceProperties();

		JSONArray propsArray = jsonObject.getJSONArray(PROPERTIES_KEY);

		for (int i = 0; i < propsArray.length(); i++) {
			JSONObject jsonProperty = propsArray.getJSONObject(i);
			// type required
			String type = jsonProperty.getString(TYPE_KEY);
			// key required
			String name = jsonProperty.getString(NAME_KEY);
			// value
			if (BYTES_TYPE.equals(type))
				// bytes so decode
				sProps.setPropertyBytes(name,
						Base64.decode(jsonProperty.getString(VALUE_KEY)));
			else if (STRING_TYPE.equals(type))
				sProps.setPropertyString(name,
						jsonProperty.getString(VALUE_KEY));
			else
				sProps.setProperty(name, jsonProperty.get(VALUE_KEY));
		}
		return new EtcdServiceInfo(location, serviceName, serviceTypeID,
				priority, weight, sProps, ttl);
	}

	public String serializeToJsonString() throws JSONException {
		JSONStringer result = new JSONStringer();
		JSONWriter stringer = result.object();
		// Location
		stringer.key(LOCATION_KEY).value(getLocation().toString());
		// priority
		stringer.key(PRIORITY_KEY).value(getPriority());
		// weight
		stringer.key(WEIGHT_KEY).value(getWeight());
		// servicename
		stringer.key(SERVICENAME_KEY).value(getServiceName());
		// ttl
		stringer.key(TTL_KEY).value(getTTL());

		// service type id
		IServiceTypeID stid = getServiceID().getServiceTypeID();
		// new object
		stringer.key(SERVICETYPE_KEY).object();
		// services array
		stringer.key(SERVICETYPE_SERVICES_KEY);
		stringer.array();
		String[] services = stid.getServices();
		for (int i = 0; i < services.length; i++)
			stringer.value(services[i]);
		stringer.endArray();
		// scopes
		stringer.key(SERVICETYPE_SCOPES_KEY);
		stringer.array();
		String[] scopes = stid.getScopes();
		for (int i = 0; i < scopes.length; i++)
			stringer.value(scopes[i]);
		stringer.endArray();
		// protocols
		stringer.key(SERVICETYPE_PROTOCOLS_KEY);
		stringer.array();
		String[] protocols = stid.getProtocols();
		for (int i = 0; i < protocols.length; i++)
			stringer.value(protocols[i]);
		stringer.endArray();
		// naming authority
		stringer.key(SERVICETYPE_NA_KEY).value(stid.getNamingAuthority());
		// end service type id
		stringer.endObject();

		// service properties
		IServiceProperties properties = getServiceProperties();

		JSONWriter propertiesWriter = stringer.key(PROPERTIES_KEY).array();
		for (@SuppressWarnings("rawtypes")
		Enumeration e = properties.getPropertyNames(); e.hasMoreElements();) {
			String key = (String) e.nextElement();
			String type = null;
			Object value = null;
			byte[] bytes = properties.getPropertyBytes(key);
			if (bytes != null) {
				value = new String(Base64.encode(bytes));
				type = BYTES_TYPE;
			} else {
				value = properties.getPropertyString(key);
				if (value != null)
					type = STRING_TYPE;
				else {
					type = OTHER_TYPE;
					value = properties.getProperty(key);
				}
			}
			JSONWriter propertyWriter = null;
			if (value != null) {
				propertyWriter = propertiesWriter.object();
				propertyWriter.key(TYPE_KEY).value(type);
				propertyWriter.key(NAME_KEY).value(key);
				propertyWriter.key(VALUE_KEY).value(value);
				propertyWriter.endObject();
			}
		}
		// end array
		propertiesWriter.endArray();
		// end entire thing
		stringer.endObject();
		return result.toString();
	}

	public EtcdServiceInfo(String jsonString) {
	}

	public EtcdServiceInfo(URI anURI, String aServiceName,
			IServiceTypeID aServiceTypeID) {
		super(anURI, aServiceName, aServiceTypeID);
	}

	public EtcdServiceInfo(URI anURI, String aServiceName,
			IServiceTypeID aServiceTypeID, IServiceProperties props) {
		super(anURI, aServiceName, aServiceTypeID, props);
	}

	public EtcdServiceInfo(URI anURI, String aServiceName,
			IServiceTypeID aServiceTypeID, int priority, int weight,
			IServiceProperties props) {
		super(anURI, aServiceName, aServiceTypeID, priority, weight, props);
	}

	public EtcdServiceInfo(URI anURI, String aServiceName,
			IServiceTypeID aServiceTypeID, int priority, int weight,
			IServiceProperties props, long ttl) {
		super(anURI, aServiceName, aServiceTypeID, priority, weight, props, ttl);
	}

	public EtcdServiceInfo(IServiceInfo serviceInfo) {
		this(serviceInfo.getLocation(), serviceInfo.getServiceName(),
				serviceInfo.getServiceID().getServiceTypeID(), serviceInfo
						.getPriority(), serviceInfo.getWeight(), serviceInfo
						.getServiceProperties(), serviceInfo.getTTL());
	}

}
