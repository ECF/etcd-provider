package org.eclipse.ecf.tests.provider.etcd;

import java.util.Arrays;
import java.util.Enumeration;

import org.eclipse.ecf.discovery.IDiscoveryAdvertiser;
import org.eclipse.ecf.discovery.IDiscoveryLocator;
import org.eclipse.ecf.discovery.IServiceInfo;
import org.eclipse.ecf.discovery.IServiceProperties;
import org.eclipse.ecf.discovery.identity.IServiceID;
import org.eclipse.ecf.internal.provider.etcd.protocol.EtcdDeleteRequest;
import org.eclipse.ecf.internal.provider.etcd.protocol.EtcdGetRequest;
import org.eclipse.ecf.internal.provider.etcd.protocol.EtcdNode;
import org.eclipse.ecf.internal.provider.etcd.protocol.EtcdResponse;
import org.eclipse.ecf.internal.provider.etcd.protocol.EtcdSetRequest;
import org.eclipse.ecf.internal.provider.etcd.protocol.EtcdSuccessResponse;
import org.eclipse.ecf.provider.etcd.EtcdDiscoveryContainerInstantiator;
import org.eclipse.ecf.provider.etcd.EtcdServiceInfo;
import org.eclipse.ecf.provider.etcd.identity.EtcdNamespace;
import org.eclipse.ecf.tests.discovery.AbstractDiscoveryTest;
import org.eclipse.ecf.tests.discovery.Activator;

@SuppressWarnings("restriction")
public class DiscoveryTest extends AbstractDiscoveryTest {

	public static final String TEST_HOST = System.getProperty(
			"etcd.test.hostname", "composent.com");
	public static final String TEST_PORT = System.getProperty("etcd.test.port",
			"2379");
	public static final String TEST_URL_BASE = System.getProperty(
			"etcd.test.getdirurl", "http://" + TEST_HOST + ":" + TEST_PORT
					+ "/v2/keys/");

	public static final String GET_SUCCEED = TEST_URL_BASE;
	public static final String GET_FAIL = System.getProperty(
			"ectd.test.fail.url", "http://" + TEST_HOST + ":" + TEST_PORT
					+ "/v2/keys/" + System.currentTimeMillis());

	public static final String SET_SUCCEED = GET_SUCCEED
			+ DiscoveryTest.class.getName();

	public static final String SETDIR_SUCCEED = GET_SUCCEED;

	public DiscoveryTest() {
		super(EtcdDiscoveryContainerInstantiator.NAME);
	}

	@Override
	protected void setUp() throws Exception {
		new EtcdNamespace();
		super.setUp();
	}

	@Override
	protected void tearDown() throws Exception {
		super.tearDown();
	}

	@Override
	protected IDiscoveryLocator getDiscoveryLocator() {
		return Activator.getDefault().getDiscoveryLocator(containerUnderTest);
	}

	@Override
	protected IDiscoveryAdvertiser getDiscoveryAdvertiser() {
		return Activator.getDefault()
				.getDiscoveryAdvertiser(containerUnderTest);
	}

	protected void deleteAll() throws Exception {
		EtcdResponse response = new EtcdGetRequest(TEST_URL_BASE, true)
				.execute();
		assertFalse(response.isError());
		EtcdSuccessResponse r = response.getSuccessResponse();
		EtcdNode node = r.getNode();
		assertNotNull(node);
		for (EtcdNode n : node.getNodes()) {
			System.out.println("deleting node with key=" + n.getKey()
					+ ", isDir=" + n.isDirectory() + ", value=" + n.getValue());
			EtcdResponse resp = null;
			if (n.isDirectory()) {
				resp = new EtcdDeleteRequest(TEST_URL_BASE + n.getKey(), true)
						.execute();
			} else {
				resp = new EtcdDeleteRequest(TEST_URL_BASE + n.getKey())
						.execute();
			}
			assertNotNull(resp);
		}
	}

	public void testAdvertiseServiceInfo() throws Exception {
		IDiscoveryAdvertiser advertiser = getDiscoveryAdvertiser();
		advertiser.registerService(this.serviceInfo);
		// sleep for while
		try {
			Thread.sleep(10000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		advertiser.unregisterAllServices();
		// sleep for while
		try {
			Thread.sleep(10000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
	
	public void testLocatorServiceInfo() throws Exception {
		IDiscoveryAdvertiser advertiser = getDiscoveryAdvertiser();
		advertiser.registerService(this.serviceInfo);
		// sleep for while
		try {
			Thread.sleep(5000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		IDiscoveryLocator locator = getDiscoveryLocator();
		IServiceInfo[] serviceInfos = locator.getServices();
		assertTrue(serviceInfos.length > 0);
		try {
			Thread.sleep(5000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		advertiser.unregisterAllServices();
		// sleep for while
		try {
			Thread.sleep(10000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
	

	public void testGetRequestSucceed() throws Exception {
		System.out.println("testGetRequestSucceed(" + GET_SUCCEED + ")");
		EtcdResponse response = new EtcdGetRequest(GET_SUCCEED, false)
				.execute();
		assertFalse(response.isError());
		System.out.println("testGetRequestSucceed(response="
				+ response.getSuccessResponse() + ")");
	}

	public void testGetRequestSucceedRecursive() throws Exception {
		System.out.println("testGetRequestSucceedRecursive(" + GET_SUCCEED
				+ ")");
		EtcdResponse response = new EtcdGetRequest(GET_SUCCEED, true).execute();
		assertFalse(response.isError());
		System.out.println("testGetRequestSucceedRecursive(response="
				+ response.getSuccessResponse() + ")");
	}

	public void testGetRequestError() throws Exception {
		EtcdGetRequest request = new EtcdGetRequest(GET_FAIL);
		EtcdResponse response = request.execute();
		assertTrue(response.isError());
	}

	public void testCreateSucceed() throws Exception {
		System.out.println("testCreateSucceed(" + SET_SUCCEED + ")");
		EtcdResponse response = new EtcdSetRequest(SET_SUCCEED,
				"hello this is new value").execute();
		assertFalse(response.isError());
		System.out.println("testCreateSucceed(response="
				+ response.getSuccessResponse() + ")");
	}

	public void testCreateDirSucceed() throws Exception {
		System.out.println("testCreateDirSucceed(" + SETDIR_SUCCEED + ")");
		EtcdResponse response = new EtcdSetRequest(SETDIR_SUCCEED
				+ String.valueOf(System.currentTimeMillis())).execute();
		assertFalse(response.isError());
		System.out.println("testCreateDirSucceed(response="
				+ response.getSuccessResponse() + ")");
	}

	public void testSerializeAndDeserializeServiceInfo() throws Exception {

		EtcdServiceInfo sinfo = new EtcdServiceInfo(serviceInfo);

		String s = sinfo.serializeToJsonString();
		assertNotNull(s);

		EtcdServiceInfo newSinfo = EtcdServiceInfo.deserializeFromString(s);

		assertNotNull(newSinfo);

		IServiceID sid1 = sinfo.getServiceID();
		IServiceID sid2 = newSinfo.getServiceID();
		assertTrue(sid1.getServiceTypeID().equals(sid2.getServiceTypeID()));
		assertTrue(sid1.equals(sid2));
		assertTrue(sinfo.getLocation().equals(newSinfo.getLocation()));
		assertTrue(sinfo.getServiceName().equals(newSinfo.getServiceName()));
		assertTrue(sinfo.getPriority() == newSinfo.getPriority());
		assertTrue(sinfo.getWeight() == newSinfo.getWeight());
		assertTrue(sinfo.getTTL() == newSinfo.getTTL());
		// get and compare service properties
		IServiceProperties sp1 = sinfo.getServiceProperties();
		IServiceProperties sp2 = newSinfo.getServiceProperties();
		assertTrue(sp1.size() == sp2.size());
		for (Enumeration<?> e1 = sp1.getPropertyNames(); e1.hasMoreElements();) {
			String key = (String) e1.nextElement();
			assertTrue(foundKey(sp2.getPropertyNames(), key));
			// try bytes
			byte[] b1 = sp1.getPropertyBytes(key);
			if (b1 != null) {
				compareByteArray(b1, sp2.getPropertyBytes(key));
			} else {
				String s1 = sp1.getPropertyString(key);
				if (s1 != null) {
					assertTrue(s1.equals(sp2.getPropertyString(key)));
				} else {
					Object o1 = sp1.getProperty(key);
					Object o2 = sp2.getProperty(key);
					assertTrue(o1.getClass().equals(o2.getClass()));
					assertTrue(o1.equals(o2));
				}
			}
		}
	}

	void compareByteArray(byte[] b1, byte[] b2) {
		// compare size
		assertTrue(b1.length == b2.length);
		for (int i = 0; i < b1.length; i++)
			if (b1[i] != b2[i])
				fail("bytes i=" + i + " of b1=" + Arrays.asList(b1) + " b2="
						+ Arrays.asList(b2) + " not equal");
	}

	boolean foundKey(Enumeration<?> e, String key) {
		for (; e.hasMoreElements();) {
			String el = (String) e.nextElement();
			if (key.equals(el))
				return true;
		}
		return false;
	}
	
	public void testDeleteAll() throws Exception {
		deleteAll();
	}
}
