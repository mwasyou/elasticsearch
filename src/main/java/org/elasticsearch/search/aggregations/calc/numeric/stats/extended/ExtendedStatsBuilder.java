package org.elasticsearch.search.aggregations.calc.numeric.stats.extended;

import org.elasticsearch.search.aggregations.calc.numeric.NumericAggregationBuilder;

/**
 *
 */
public class ExtendedStatsBuilder extends NumericAggregationBuilder<ExtendedStatsBuilder> {

    public ExtendedStatsBuilder(String name) {
        super(name, InternalExtendedStats.TYPE.name());
    }
}