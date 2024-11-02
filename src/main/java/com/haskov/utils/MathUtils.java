package com.haskov.utils;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

public class MathUtils {

    public static Pair<Double, Double> getQuadraticRoots(double a, double b, double c) {
        double discriminant = b * b - 4 * a * c;
        return new ImmutablePair<>((- b - discriminant) / 2 * a, (- b + discriminant) / 2 * a);
    }
}
