package com.haskov.bench.v2;

import org.slf4j.Logger;

import java.util.List;

public class Results {

	public List<Snap> raw;
	
	public int period;
	
	public Long durationNs;
	public Long iterations;
	public Long tps;
	public Long tpsLast5sec;
	
	public Results(List<Snap> raw, int period, long durationNs) {
		this.raw = raw;
		this.period = period;
		this.durationNs = durationNs;
		
		Snap latest = raw.get(raw.size() - 1);
		Snap sec5 = raw.get(Math.max(raw.size() - 1 - 5000 / period, 0));
		
		iterations = latest.iterations;
		tps = latest.iterations * 1000000000 / latest.ts;
		tpsLast5sec = ((latest.iterations - sec5.iterations) * 1000000000 / Math.max(1, latest.ts - sec5.ts));
	}
	
	public boolean similar(Results another) {
		double err = (2.0 * Math.abs(this.tpsLast5sec - another.tpsLast5sec)) / Math.abs(this.tpsLast5sec + another.tpsLast5sec);
		return (err < 0.1);
	}
	
	public void logSummary(Logger log, String intro) {
		log.info("{} {} msec:\n - {} iterations\n - {} tps (overall) \n - {} tps (last 5 sec)", intro,
				durationNs / 1000000 ,this.iterations, this.tps, this.tpsLast5sec);
	}
}
