package com.acme.fn;

import org.apache.flink.table.functions.AggregateFunction;

/** Weighted average: SUM(value * weight) / SUM(weight). Retractable, so it also runs over updating input. */
public class WeightedAvg extends AggregateFunction<Double, WeightedAvg.Acc> {
    public static class Acc {
        public double sum = 0;
        public double weight = 0;
    }

    @Override
    public Acc createAccumulator() {
        return new Acc();
    }

    public void accumulate(Acc acc, Double value, Double weight) {
        if (value != null && weight != null) {
            acc.sum += value * weight;
            acc.weight += weight;
        }
    }

    public void retract(Acc acc, Double value, Double weight) {
        if (value != null && weight != null) {
            acc.sum -= value * weight;
            acc.weight -= weight;
        }
    }

    @Override
    public Double getValue(Acc acc) {
        return acc.weight == 0 ? null : acc.sum / acc.weight;
    }
}
