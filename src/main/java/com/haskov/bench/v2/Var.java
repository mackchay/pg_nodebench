package com.haskov.bench.v2;

import java.util.Random;

public class Var {
	public Random rnd = new Random();
	public Long value;
	
	public Long start = 0L;
	public Long end = Long.MAX_VALUE;
	
	public Long get() {
		if (rnd == null)
			return value;
		
		if (end == Long.MAX_VALUE && start == 0L) {
			return rnd.nextLong();
		}
		
		return (long) (start + rnd.nextFloat() * (end - start));
	}
	
	
	public void set(Long x) {
		value = x;
	}
	
	public Long min() {
		return start;
	}
	
	public Long max() {
		return end;
	}
	
	@Override
	public String toString() {
		return get().toString();
	}
}
