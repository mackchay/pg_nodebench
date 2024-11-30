package com.haskov.utils;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class MathUtils {

    public static List<Boolean> getRandomBooleanList(int size) {
        Random random = new Random();
        List<Boolean> list = new ArrayList<>();

        for (int i = 0; i < size; i++) {
            list.add(random.nextDouble() < 0.5);
        }

        return list;
    }
}
