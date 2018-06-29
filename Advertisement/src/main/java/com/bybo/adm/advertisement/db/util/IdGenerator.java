package com.bybo.adm.advertisement.db.util;

public class IdGenerator {

	/**
	 * Twitter_Snowflake<br>
	 * SnowFlake的结构如下(每部分用-分开):<br>
	 * 0 - 0000000000 0000000000 0000000000 0000000000 0 - 00000 - 00000 -
	 * 000000000000 <br>
	 * 1位标识，由于long基本类型在Java中是带符号的，最高位是符号位，正数是0，负数是1，所以id一般是正数，最高位是0<br>
	 * 41位时间截(毫秒级)，注意，41位时间截不是存储当前时间的时间截，而是存储时间截的差值（当前时间截 - 开始时间截)
	 * 得到的值），这里的的开始时间截，一般是我们的id生成器开始使用的时间，由我们程序来指定的（如下下面程序IdWorker类的startTime属性）
	 * 。41位的时间截，可以使用69年，年T = (1L << 41) / (1000L * 60 * 60 * 24 * 365) = 69<br>
	 * 10位的数据机器位，可以部署在1024个节点，包括5位datacenterId和5位workerId<br>
	 * 12位序列，毫秒内的计数，12位的计数顺序号支持每个节点每毫秒(同一机器，同一时间截)产生4096个ID序号<br>
	 * 加起来刚好64位，为一个Long型。<br>
	 * SnowFlake的优点是，整体上按照时间自增排序，并且整个分布式系统内不会产生ID碰撞(由数据中心ID和机器ID作区分)，并且效率较高，经测试，
	 * SnowFlake每秒能够产生26万ID左右。
	 */

	// ====== Fields =====

	// Start timestamp
	private final long startTimestamp = 142004160000L;

	// Worker id
	private final long workerIdBits = 5L;

	// Data center id bits
	private final long dataCenterIdBits = 5L;

	// Max Worker id bits
	private final long maxWorkerId = -1L ^ (-1L << workerIdBits);

	// Max data center id
	private final long maxDataCenterId = -1L ^ (-1L << dataCenterIdBits);

	// Sequence Bits
	private final long sequenceBits = 12L;

	// Worker Id shift 12 bits
	private final long workerIdShift = sequenceBits;

	// Center id shift 17 bits
	private final long dataCenterIdShift = sequenceBits + workerIdBits;

	// Timestamp shift
	private final long timestampLeftShift = sequenceBits + workerIdBits + dataCenterIdBits;

	// Sequence Mash
	private final long sequenceMask = -1L ^ (-1L << sequenceBits);

	// Worker Id (0 ~ 31)
	private long workerId;

	// Data center Id (0 ~ 31)
	private long dataCenterId;

	// Sequence (0 ~ 4095)
	private long sequence = 0L;

	// Last timestamp
	private long lastTimestamp = -1l;

	public IdGenerator(long workerId, long dataCenterId){
	        if(workerId > this.maxWorkerId || workerId < 0){
	            throw new IllegalArgumentException(String.format("Worker Id can't be greater than %d or less than 0", maxWorkerId));
	        }
	        if(dataCenterId > this.maxDataCenterId || dataCenterId < 0){
	            throw new IllegalArgumentException(String.format("Data center Id can't be greater than %d or less than 0", maxDataCenterId));
	        }
	        
	        this.workerId = workerId;
	        this.dataCenterId = dataCenterId;
	    }

	// ====== Methods =====
	public synchronized long getId() {
		long timestamp = generateTime();

		if (timestamp < this.lastTimestamp) {
			throw new RuntimeException(
					String.format("Clock moved backwards. Refusing to generate if for %d milliseconds",
							this.lastTimestamp - timestamp));
		}
		if (timestamp == this.lastTimestamp) {
			this.sequence = (this.sequence + 1) & sequenceMask;
			if (sequence == 0) {
				timestamp = this.tilNextMillis(this.lastTimestamp);
			}
		} else {
			this.sequence = 0L;
		}

		this.lastTimestamp = timestamp;

		return ((timestamp - this.startTimestamp) << this.timestampLeftShift)
				| (this.dataCenterId << this.dataCenterIdShift) | (this.workerId << this.workerIdShift) | this.sequence;
	}

	protected long generateTime() {
		return System.currentTimeMillis();
	}

	protected long tilNextMillis(long lastTimestamp) {
		long timestamp = this.generateTime();
		while (timestamp <= this.lastTimestamp) {
			timestamp = this.generateTime();
		}
		return timestamp;
	}

}
