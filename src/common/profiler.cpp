#include "src/common/include/profiler.h"

#include <cassert>

namespace graphflow {
namespace common {

TimeMetric* Profiler::registerTimeMetric(const string& key) {
    auto timeMetric = make_unique<TimeMetric>(enabled);
    auto metricPtr = timeMetric.get();
    addMetric(key, move(timeMetric));
    return metricPtr;
}

NumericMetric* Profiler::registerNumericMetric(const string& key) {
    auto numericMetric = make_unique<NumericMetric>(enabled);
    auto metricPtr = numericMetric.get();
    addMetric(key, move(numericMetric));
    return metricPtr;
}

double Profiler::sumAllTimeMetricsWithKey(const string& key) {
    auto sum = 0.0;
    assert(metrics.contains(key));
    for (auto& metric : metrics.at(key)) {
        sum += ((TimeMetric*)metric.get())->accumulatedTime;
    }
    return sum;
}

uint64_t Profiler::sumAllNumericMetricsWithKey(const string& key) {
    auto sum = 0ul;
    assert(metrics.contains(key));
    for (auto& metric : metrics.at(key)) {
        sum += ((NumericMetric*)metric.get())->accumulatedValue;
    }
    return sum;
}

void Profiler::resetMetrics() {
    metrics.clear();
}

void Profiler::addMetric(const string& key, unique_ptr<Metric> metric) {
    if (!metrics.contains(key)) {
        metrics.insert({key, vector<unique_ptr<Metric>>()});
    }
    metrics.at(key).push_back(move(metric));
}

} // namespace common
} // namespace graphflow
