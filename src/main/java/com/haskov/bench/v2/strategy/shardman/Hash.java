package com.haskov.bench.v2.strategy.shardman;

/**
 * @author Ivan Frolkov
 */

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.UUID;

public class Hash {
	public static final long PartitionSeed = 0x7A5B22367996DCFDL;
	final static int _a = 0;
	final static int _b = 1;
	final static int __b = 0;
	final static int _c = 2;
	final static int __c = 1;

	static int rot32(int in, int shift) {
		return Integer.rotateLeft(in, shift);
	}

	static int[] mix32(int a, int b, int c) {
		a -= c;
		a ^= rot32(c, 4);
		c += b;
		b -= a;
		b ^= rot32(a, 6);
		a += c;
		c -= b;
		c ^= rot32(b, 8);
		b += a;
		a -= c;
		a ^= rot32(c, 16);
		c += b;
		b -= a;
		b ^= rot32(a, 19);
		a += c;
		c -= b;
		c ^= rot32(b, 4);
		b += a;
		return new int[] { a, b, c };
	}

	static int[] mix32(int[] abc) {
		return mix32(abc[_a], abc[_b], abc[_c]);
	}

	static int[] final32(int a, int b, int c) {
		c ^= b;
		c -= rot32(b, 14);
		a ^= c;
		a -= rot32(c, 11);
		b ^= a;
		b -= rot32(a, 25);
		c ^= b;
		c -= rot32(b, 16);
		a ^= c;
		a -= rot32(c, 4);
		b ^= a;
		b -= rot32(a, 14);
		c ^= b;
		c -= rot32(b, 24);
		return new int[] { b, c };
	}

	static int[] final32(int[] abc) {
		return final32(abc[_a], abc[_b], abc[_c]);
	}

	static int[] initABC(int size) {
		int a = 0x9e3779b9 + size + 3923095;
		return new int[] { a, a, a };
	}

	public static long hashInt(int k, long seed) {
		int[] abc = initABC(4);

		if (seed != 0) {
			abc[_a] += (int) (seed >>> 32);
			abc[_b] += (int) (seed);
			abc = mix32(abc);
		}

		abc[_a] += k;
		int[] bc = final32(abc);

		return (long) bc[__b] << 32 | Integer.toUnsignedLong(bc[__c]);
	}

	public static long hashBytes(byte[] bytes, long seed) {
		int[] abc = initABC(bytes.length);

		if (seed != 0) {
			abc[_a] += (int) (seed >>> 32);
			abc[_b] += (int) (seed);
			abc = mix32(abc);
		}

		int len = 0;
		while (bytes.length - len - 1 >= 12) {
			abc[_a] += ByteBuffer.wrap(bytes, len, 4).order(ByteOrder.LITTLE_ENDIAN).getInt(); // 2691335425
			abc[_b] += ByteBuffer.wrap(bytes, len + 4, 4).order(ByteOrder.LITTLE_ENDIAN).getInt();
			abc[_c] += ByteBuffer.wrap(bytes, len + 8, 4).order(ByteOrder.LITTLE_ENDIAN).getInt();
			len += 12;
			abc = mix32(abc);
		}
		if (len < bytes.length) {
			byte[] bytes1 = new byte[12];
			System.arraycopy(bytes, len, bytes1, 0, bytes.length - len);

			abc[_a] += ByteBuffer.wrap(bytes1, 0, 4).order(ByteOrder.LITTLE_ENDIAN).getInt();
			abc[_b] += ByteBuffer.wrap(bytes1, 4, 4).order(ByteOrder.LITTLE_ENDIAN).getInt();
			abc[_c] += ByteBuffer.wrap(bytes1, 8, 4).order(ByteOrder.LITTLE_ENDIAN).getInt();
		}

		int[] bc = final32(abc);

		return ((long) bc[__b]) << 32 | Integer.toUnsignedLong(bc[__c]);
	}

	public static long hashCombine64(long a, long b) {
		return a ^ (b + 0x49a0f4dd15e5a8e3L + (a << 54) + (a >>> 7));
	}

	public static long hashChar(byte c, long seed) {
		return hashInt((int) c, seed);
	}

	static public long hashBool(boolean val, long seed) {
		return hashInt(val ? 1 : 0, seed);
	}

	static public long hashInt2(short val, long seed) {
		return hashInt((int) val, seed);
	}

	static public long hashInt4(int val, long seed) {
		return hashInt(val, seed);
	}

	static public long hashInt8(long val, long seed) {
		ByteBuffer bb = ByteBuffer.allocate(8);
		bb.putLong(val);

		int hipart = bb.getInt();
		int lopart = bb.getInt();

		if (val >= 0) {
			lopart ^= hipart;
		} else {
			lopart ^= ~hipart;
		}
		return hashInt(lopart, seed);
	}

	public static long hashUuid(UUID uuid, long seed) {
		ByteBuffer bb = ByteBuffer.allocate(16);
		bb.putLong(uuid.getMostSignificantBits());
		bb.putLong(uuid.getLeastSignificantBits());
		return hashBytes(bb.array(), seed);
	}

	public static long hashFloat32(float val, long seed) {
		if (val == 0) {
			return seed;
		}

		double dval = val;
		if (Double.isNaN(dval)) {
			dval = Double.NaN;
		}
		ByteBuffer bb = ByteBuffer.allocate(8);
		bb.putDouble(dval);
		return hashBytes(bb.array(), seed);
	}

	public static long hashFloat64(double val, long seed) {
		if (val == 0) {
			return seed;
		}

		if (Double.isNaN(val)) {
			val = Double.NaN;
		}
		ByteBuffer bb = ByteBuffer.allocate(8);
		bb.putDouble(val);
		return hashBytes(bb.array(), seed);
	}

}
