#include "src/common/include/metric.h"

namespace graphflow {
namespace common {

TimeMetric::TimeMetric(bool enable) : Metric(enable) {
    accumulatedTime = 0;
    isStarted = false;
    timer = Timer();
}

void TimeMetric::start() {
    if (!enabled) {
        return;
    }
    isStarted = true;
    timer.start();
}

void TimeMetric::stop() {
    if (!enabled) {
        return;
    }
    if (!isStarted) {
        throw invalid_argument("Timer metric has not started.");
    }
    timer.stop();
    accumulatedTime += timer.getDuration();
    isStarted = false;
}

NumericMetric::NumericMetric(bool enable) : Metric(enable) {
    accumulatedValue = 0u;
}

void NumericMetric::increase(uint64_t value) {
    if (!enabled) {
        return;
    }
    accumulatedValue += value;
}

} // namespace common
} // namespace graphflow
