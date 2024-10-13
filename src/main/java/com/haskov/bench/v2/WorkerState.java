package com.haskov.bench.v2;

import java.util.Random;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;

public class WorkerState {
	public Random rnd = new Random();
	
	public long start;
	public long curr;
	public long end;
	public long iterationsDone;
	public long iterationLimit;
	public CyclicBarrier startPoint;
	public Semaphore concurrentLimit;
	public AtomicBoolean stop = new AtomicBoolean(false);
}
