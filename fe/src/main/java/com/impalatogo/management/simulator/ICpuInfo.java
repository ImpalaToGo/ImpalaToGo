package com.impalatogo.management.simulator;

public interface ICpuInfo {

	public abstract String getModelName();

	public abstract String getVendor();

	public abstract CPUCoreInfo[] getCores();

	public abstract double getCpuLoad();

	public abstract int getNumOfCores();

	public abstract int getNumOfThreads();

	public abstract long getSpeedMeasure();

	public class CPUCoreInfo {
		public int id;// serial number of core in CPU

		public int currentFreq() {
			return (int) ((Math.random() * 30 + 1) * 100) * 1024 * 1024;
		};// Hz

		public long maxFreq;// Hz
		public int physicalBusSize;// bits
		public int virtualBusSize;// bits
		public int numTheads;
		public float bogoMips;
		public int cacheSizeKb;// in Kb
	}
}