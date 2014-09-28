package com.impalatogo.management.simulator.tests;

import static org.junit.Assert.*;

import java.util.List;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;

import com.impalatogo.management.simulator.IClusterNodeInfo;
import com.impalatogo.management.simulator.IClusterState;
import com.impalatogo.management.simulator.ICpuInfo;
import com.impalatogo.management.simulator.ICpuInfo.CPUCoreInfo;
import com.impalatogo.management.simulator.IImpalaTaskInfo;
import com.impalatogo.management.simulator.IProcessResourceInfo;
import com.impalatogo.management.simulator.IStorageInfo;
import com.impalatogo.management.simulator.RandomValuesApi;

public class ApiTest {
	IClusterNodeInfo cn;
	IClusterState cs;

	@Before
	public void before() {
		cn = RandomValuesApi.getRandomClusterNode();
		cs = RandomValuesApi.getRandomClusterState();
	}

	@Test
	public void testRandomClusterNodeIntegrity() {
		assertNotNull(cn);
		assertNotNull(cn.getCPUInfo());
		assertNotNull(cn.getStorageInfo());
		assertNotNull(cn.getNodeName());

	}

	@Test
	public void testClusterNodeCpuInfoIntegrity() throws Exception {
		ICpuInfo cpuInfo = cn.getCPUInfo();
		assertCpuInfoIntegrity(cpuInfo);
	}

	private void assertCpuInfoIntegrity(ICpuInfo cpuInfo) {
		assertNotNull(cpuInfo.getCores());
		assertNotNull(cpuInfo.getModelName());
		assertNotNull(cpuInfo.getVendor());
		assertNotEquals(0, cpuInfo.getCpuLoad());
		assertNotEquals(0, cpuInfo.getCores().length);
		assertNotEquals(0, cpuInfo.getNumOfCores());
		assertEquals(cpuInfo.getCores().length, cpuInfo.getNumOfCores());
		assertNotEquals(0, cpuInfo.getNumOfThreads());
		assertTrue(cpuInfo.getNumOfThreads() >= cpuInfo.getNumOfCores());
		assertNotEquals(0, cpuInfo.getSpeedMeasure());
	}

	@Test
	public void testStorageInfoIntegrity() throws Exception {
		List<IStorageInfo> storagInfo = cn.getStorageInfo();
		assertNotEquals(
				String.format("storage info is empty for node \"%s\"",
						cn.getNodeName()), 0, storagInfo.size());
	}

	@Test
	public void testNetworkInfoIntegrity() throws Exception {
		assertTrue(cn.getNetworkBandwidthInsideCluster() > 0);
		assertTrue(cn.getNetworkBandwidthToDFS() > 0);
		assertTrue(cn.getNetworkBandwidthInsideCluster() > cn
				.getNetworkBandwidthToDFS());
	}

	@Test
	public void testRandomClusterStateIntegrity() throws Exception {
		assertNotNull(cs);
		assertNotNull(cs.getCurrentProcesses());
		assertNotNull(cs.getNodesInfo());
	}

	@Test
	public void testProcessesIntegrity() throws Exception {
		assertNotEquals(0, cs.getCurrentProcesses().size());

		for (IImpalaTaskInfo pi : cs.getCurrentProcesses()) {
			assertProcessInfoIntegrity(pi);
			Map<String, IProcessResourceInfo> pripn = cs
					.getProcessResourcesById(pi.getProcessID());
			assertNotNull(pripn);
			assertFalse(
					String.format("Process info is empty for process %s",
							pi.getProcessID()), pripn.isEmpty());
		}
	}

	@Test
	public void testProcessInOurCluster() throws Exception {
		for (IImpalaTaskInfo pi : cs.getCurrentProcesses()) {
			List<IClusterNodeInfo> nis = cs.getNodesInfo();
			Map<String, IProcessResourceInfo> pripn = cs
					.getProcessResourcesById(pi.getProcessID());
			for (String name : pripn.keySet()) {
				assertNodeNameIsInOurCluster(name, nis);
			}
		}
	}

	@Test
	public void testNodesInfoIntegrity() throws Exception {
		List<IClusterNodeInfo> nis = cs.getNodesInfo();
		for (IClusterNodeInfo ni : nis) {
			assertNotNull(ni);
			assertNodeInfo(ni);
		}
	}

	@Test
	public void testProcessResourceInfoIntegrity() throws Exception {
		for (IImpalaTaskInfo pi : cs.getCurrentProcesses()) {
			Map<String, IProcessResourceInfo> pripn = cs
					.getProcessResourcesById(pi.getProcessID());
			for (IProcessResourceInfo pri : pripn.values()) {
				assertNotNull(pri);
				assertProcessResourceInfoIntergity(pri);
			}
		}
	}

	@Test
	public void testClusterNodeHiddenAssumptions1() throws Exception {

	}

	@Test
	public void testClusterStateHiddenAssumptions() throws Exception {
		assertTrue(cn.getStorageInfo().size() >= 1);
		assertTrue(cn.getStorageInfo().size() <= 22);
	}

	@Test
	public void testCpuCoresHiddenAssumptions() throws Exception {
		ICpuInfo cpui = cn.getCPUInfo();
		assertTrue(
				String.format("CPU speed is %d instead of %d",
						cpui.getSpeedMeasure(), 40l * 100 * 1024 * 1024),
				cpui.getSpeedMeasure() <= 40l * 100 * 1024 * 1024);// 4GHz
		assertTrue(cpui.getSpeedMeasure() > 1l * 1024 * 1024);// 1MHz - I guess
																// you'll not
																// run on
																// somthing so
																// old
		int coresCount = 0;
		int threadCount = 0;
		for (CPUCoreInfo cpuci : cpui.getCores()) {
			coresCount++;
			threadCount += cpuci.numTheads;
			assertTrue("CPU ID cannot exceed number of CPUs",
					cpuci.id <= cpui.getNumOfCores());
			assertTrue(cpuci.physicalBusSize >= 32);
			assertTrue(cpuci.physicalBusSize < 45);
			assertTrue(cpuci.virtualBusSize >= 32);
			assertTrue(cpuci.virtualBusSize < 65);
			assertTrue(cpuci.maxFreq > 0);
			assertTrue(cpuci.bogoMips > 0);
			assertTrue(cpuci.cacheSizeKb > 0);
		}

		assertTrue(coresCount == cpui.getNumOfCores());
		assertTrue(threadCount == cpui.getNumOfThreads());
		assertTrue(coresCount <= 128);
		assertTrue(coresCount > 0);
		assertTrue(threadCount <= 128 * 4); // assuming at most 4 threads per
											// CPU
		assertTrue(threadCount > 0);
	}

	private void assertProcessResourceInfoIntergity(IProcessResourceInfo pri) {
		assertNotNegative(pri.getCPUCurrentUse());
		assertNotNegative(pri.getClusterBandwidthUsed());
		assertNotNegative(pri.getDFSBandwidthUsed());
		assertNotNegative(pri.getLocalDiskBandwidthUse());
		assertNotNegative(pri.getLocalDiskSpaceUse());
		assertNotNegative(pri.getMemoryUse());
	}

	private void assertNodeNameIsInOurCluster(String name,
			List<IClusterNodeInfo> nis) {
		boolean found = false;
		for (IClusterNodeInfo i : nis)
			found |= name.contentEquals(i.getNodeName());
		assertTrue(found);
	}

	private void assertNotNegative(long num) {
		assertFalse("Value is negative", num < 0);
	}

	private void assertNodeInfo(IClusterNodeInfo ni) {
		assertNotNull(ni.getNodeName());
		assertNotNegative(ni.getFreeCacheSpace());
		assertNotNegative(ni.getMemorySize());
		assertNotNegative(ni.getNetworkBandwidthInsideCluster());
		assertNotNegative(ni.getNetworkBandwidthToDFS());
		assertCpuInfoIntegrity(ni.getCPUInfo());
	}

	private void assertProcessInfoIntegrity(IImpalaTaskInfo pi) {
		assertNotNull(pi);
		assertNotNull(pi.getProcessDefinitionText());
		assertNotNull(pi.getProcessEndTime());
		assertNotNull(pi.getProcessID());
		assertNotNull(pi.getProcessStartTime());

	}
}
