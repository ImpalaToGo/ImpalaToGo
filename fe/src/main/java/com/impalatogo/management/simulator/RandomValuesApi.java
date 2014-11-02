package com.impalatogo.management.simulator;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.atomic.AtomicInteger;

public class RandomValuesApi {

	private static final class RandomClusterState implements IClusterState {
		private final class NoResourceConsumingProcessInfo implements
				IProcessResourceInfo {

			@Override
			public long getDFSBandwidthUsed() {
				// TODO Auto-generated method stub
				return 0;
			}

			@Override
			public long getClusterBandwidthUsed() {
				// TODO Auto-generated method stub
				return 0;
			}

			@Override
			public int getCPUCurrentUse() {
				// TODO Auto-generated method stub
				return 0;
			}

			@Override
			public long getLocalDiskBandwidthUse() {
				// TODO Auto-generated method stub
				return 0;
			}

			@Override
			public long getLocalDiskSpaceUse() {
				// TODO Auto-generated method stub
				return 0;
			}

			@Override
			public long getMemoryUse() {
				// TODO Auto-generated method stub
				return 0;
			}

		}

		private final class RandomProcessResourceInfo implements
				IProcessResourceInfo {
			private long memoryUse = getRandomIntLimitedBy(1024 * 1024 * 1024 * 128);
			private long localDiskUse = getRandomIntLimitedBy(1024 * 1024 * 1024 * 1024);
			private long localDiskBandwithUse = getRandomIntBetween(1024 * 2,
					1024 * 1024 * 1024);// 2KB~1GB
			private long localDfsBandwithUse = getRandomIntLimitedBy(1024 * 1024 * 1024);// 1GBit
			private long localClusterBandwithUse = getRandomIntLimitedBy(1024 * 1024 * 1024 * 100);// 100GBit
			private int cpuCurrentUse = getRandomIntLimitedBy(100);

			@Override
			public long getMemoryUse() {
				return memoryUse;
			}

			@Override
			public long getLocalDiskSpaceUse() {
				return localDiskUse;
			}

			@Override
			public long getLocalDiskBandwidthUse() {
				return localDiskBandwithUse;
			}

			@Override
			public long getDFSBandwidthUsed() {
				return localDfsBandwithUse;
			}

			@Override
			public long getClusterBandwidthUsed() {
				return localClusterBandwithUse;
			}

			@Override
			public int getCPUCurrentUse() {
				return cpuCurrentUse;
			}
		}

		private List<IImpalaTaskInfo> currentProcesses = createRandomProcesses();
		private List<IClusterNodeInfo> nodes = createRandomNodesList();
		/** Map<ProcessId, Map <nodeId,IProcessResourceInfo>> */
		private Map<String, Map<String, IProcessResourceInfo>> processResourcesPerNode = createRandomProcessResourcePerNode();

		private Map<String, Map<String, IProcessResourceInfo>> createRandomProcessResourcePerNode() {
			Map<String, Map<String, IProcessResourceInfo>> res = new HashMap<String, Map<String, IProcessResourceInfo>>();
			for (IImpalaTaskInfo info : currentProcesses) {
				Map<String, IProcessResourceInfo> m = new HashMap<String, IProcessResourceInfo>();
				for (IClusterNodeInfo n : nodes) {
					boolean processRunsOnThisNode = getRandomBoolean();
					if (!processRunsOnThisNode)
						continue;
					IProcessResourceInfo usageForThisNode = new RandomProcessResourceInfo();
					m.put(n.getNodeName(), usageForThisNode);
				}
				if (m.isEmpty())
					m.put(nodes.get(0).getNodeName(),
							new NoResourceConsumingProcessInfo());
				res.put(info.getProcessID(), m);
			}
			return res;
		}

		private List<IClusterNodeInfo> createRandomNodesList() {
			List<IClusterNodeInfo> res = new ArrayList<IClusterNodeInfo>();
			int nodesToCreate = 1 + getRandomIntLimitedBy(31);// limited by 1~32
			for (int i = 0; i < nodesToCreate; i++)
				res.add(getRandomClusterNode());
			return res;
		}

		private List<IImpalaTaskInfo> createRandomProcesses() {
			List<IImpalaTaskInfo> res = new ArrayList<IImpalaTaskInfo>();
			int processesToCreate = getRandomIntLimitedBy(1000);
			for (int i = 0; i < processesToCreate; i++)
				res.add(new RandomTaskInfo());
			return res;
		}

		@Override
		public List<IClusterNodeInfo> getNodesInfo() {
			return nodes;
		}

		@Override
		public List<IImpalaTaskInfo> getCurrentProcesses() {
			return currentProcesses;
		}

		@Override
		public Map<String, IProcessResourceInfo> getProcessResourcesById(
				String processID) {
			return processResourcesPerNode.get(processID);
		}
	}

	private static final class RandomTaskInfo implements IImpalaTaskInfo {
		private static AtomicInteger lastId = new AtomicInteger(0);
		TaskType type;
		TaskStatus status = TaskStatus.WAITING_TO_START;
		String queryText;
		Date start;
		Date end;
		String taskId;
		double completePerCent;
		Timer completTimer;

		public RandomTaskInfo() {
			taskId = String.format("task_#%d", lastId.incrementAndGet());
			long startOffset = getRandomLongLimitedBy(1000l * 300 - 1000 * 1000
					* 150);// in
			// millisecond Negative value means the task in the past
			long taskDuration = getRandomLongLimitedBy(1000l * 1000 * 10);// in
																			// millisecond.
			start = new Date();
			start.setTime(start.getTime() + startOffset);
			end = new Date(start.getTime() + taskDuration);
			completTimer = new Timer();
			completTimer.scheduleAtFixedRate(new TimerTask() {

				@Override
				public void run() {
					Date now = new Date();
					if (start.after(now))
						status = TaskStatus.WAITING_TO_START;

					if (now.after(end)) {
						completePerCent = 100;
						if (getRandomBoolean())
							status = TaskStatus.FINISHED;
						else
							status = TaskStatus.FAIELD;
						completTimer.cancel();

					} else {
						status = TaskStatus.RUNNING;
						completePerCent = 100.0
								* (start.getTime() - now.getTime())
								/ (start.getTime() - end.getTime());
					}
				}
			}, 0, 1000);

			queryText = String
					.format("The task to make world better and your queries faster. %s",
							taskId);

			type = (TaskType) generateRandomEnumValue(TaskType.class);
		}

		private static Object generateRandomEnumValue(
				Class<? extends Enum<?>> en) {
			if (!en.isEnum())
				return null;
			Object[] values = en.getEnumConstants();
			if (values == null)
				return null;
			int maxIndex = values.length;
			int index = (int) Math.floor(Math.random() * maxIndex);
			if (index == maxIndex)
				index -= 1;
			return values[index];
		}

		@Override
		public int getProgress() {
			return (int) Math.floor(completePerCent);
		}

		@Override
		public TaskType getProcessType() {
			return type;
		}

		@Override
		public Date getProcessStartTime() {
			return start;
		}

		@Override
		public String getProcessID() {
			return taskId;
		}

		@Override
		public Date getProcessEndTime() {
			return end;
		}

		@Override
		public String getProcessDefinitionText() {
			return queryText;
		}

		@Override
		public TaskStatus getTaskStatus() {
			return status;
		}
	}

	private static final class RandomCpuInfo implements ICpuInfo {
		// TODO: Enhance to more processor families and randomize
		String modelName;
		CPUCoreInfo[] cores;;

		public RandomCpuInfo() {
			cores = createCpuCores();
			modelName = createIntelServerModelName();
		}

		@Override
		public String getModelName() {
			return modelName;
		}

		private CPUCoreInfo[] createCpuCores() {
			int maxCoresDegree = 7;// 2^7=128
			int coresPerCpu = (int) Math.floor(Math.pow(2,
					Math.floor(Math.random() * maxCoresDegree)));
			int threadsPerCore = (int) Math.floor(Math.pow(2,
					Math.floor(Math.random() * 3)));// 2^{0~2} ~ 1,2,4
			CPUCoreInfo[] _cores = (CPUCoreInfo[]) Array.newInstance(
					CPUCoreInfo.class, coresPerCpu);
			for (int i = 0; i < coresPerCpu; i++) {
				_cores[i] = new CPUCoreInfo();
				_cores[i].bogoMips = (float) (1 + Math.random() * 10000);
				_cores[i].cacheSizeKb = getRandomIntBetween(1, 1024 * 10);
				_cores[i].id = i;
				_cores[i].maxFreq = 31l * 1024 * 1024 * 100;
				_cores[i].numTheads = threadsPerCore;
				_cores[i].physicalBusSize = getRandomIntBetween(32, 45);
				_cores[i].virtualBusSize = getRandomIntBetween(32, 65);
			}
			return _cores;
		}

		private String createIntelServerModelName() {
			/*
			 * http://www.intel.com/content/www/us/en/processors/processor-
			 * numbers-data-center.html
			 */
			final String intelProcServerFormat = "Intel® %s® processor %2s%1d%2d%1s%2s";
			String brand = "Xeon";// Xeon Atom Itanium
			String productLine = "E3";// E3 E5 E7
			int waynesss = 2;// maximum number CPUs in a Node (1,2,4,8)
			int socketType = 6;// 2,4,6,8
			int sku = 20;// 10,20,30,40,etc...
			boolean lowPower = false;
			int version = 2;// 1,2,3,4,etc...
			return String.format(intelProcServerFormat, brand, productLine,
					waynesss, socketType, sku, lowPower ? "L" : "", "V"
							+ version);
		}

		@Override
		public String getVendor() {
			return "Genue Intel";
		}

		@Override
		public CPUCoreInfo[] getCores() {
			return cores;
		}

		@Override
		public double getCpuLoad() {
			return Math.random() * (getNumOfThreads() + 1);
		}

		@Override
		public int getNumOfCores() {
			return cores.length;
		}

		@Override
		public int getNumOfThreads() {
			int res = 0;
			for (CPUCoreInfo core : cores)
				res += core.numTheads;
			return res;
		}

		@Override
		public long getSpeedMeasure() {
			long max = 0;
			for (CPUCoreInfo core : cores)
				max = Math.max(max, core.maxFreq);
			return max;
		}
	}

	private static final class RandomStorageInfo implements IStorageInfo {
		boolean isLocal = getRandomBoolean();
		long writeTime = getRandomLongLimitedBy(1000000000l);
		long readTime = getRandomLongLimitedBy(1000000000l);
		long writeBw = getRandomLongLimitedBy(1024l * 1024 * 500);// max=500MBpS
		long readBw = getRandomLongLimitedBy(1024l * 1024 * 500);// max=500MBpS
		long iops = getRandomLongLimitedBy(1024l);// max=1000IOPS - it's a big
													// number for IOPS...

		@Override
		public boolean isLocal() {
			return isLocal;
		}

		@Override
		public long getWriteTime() {
			return writeTime;
		}

		@Override
		public long getWriteBandwidth() {
			return writeBw;
		}

		@Override
		public long getReadTime() {
			return readTime;
		}

		@Override
		public long getReadBandwidth() {
			return readBw;
		}

		@Override
		public long getIOPs() {
			return iops;
		}
	}

	public static IClusterNodeInfo getRandomClusterNode() {
		TrivialClusterNodeInfo cn = new TrivialClusterNodeInfo();
		cn.setCpuInfo(new RandomCpuInfo());
		int cntStorageInfo = getRandomIntBetween(1, 22);
		for (int i = 0; i < cntStorageInfo; i++)
			cn.getStorageInfo().add(new RandomStorageInfo());
		return cn;
	}

	public static IClusterState getRandomClusterState() {
		return new RandomClusterState();
	}

	private static int getRandomIntBetween(int min, int max) {
		return (int) (min + Math.random() * (max - min));
	}

	private static int getRandomIntLimitedBy(int limit) {
		return (int) (Math.random() * limit);
	}

	private static boolean getRandomBoolean() {
		return Math.random() > 0.5;
	}

	private static long getRandomLongLimitedBy(long max) {
		return (long) (Math.random() * max);
	}

}
