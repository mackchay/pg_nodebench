package com.haskov.bench.v2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Random;

public class RandomList<E> extends ArrayList<E> {

	private final Random rand = new Random();
	private static final long serialVersionUID = 321L;

	@SafeVarargs
	public RandomList(E... e) {
		super(Arrays.asList(e));
	}
	
	public E get() {
		return get(rand.nextInt(this.size()));
	}
	
}
