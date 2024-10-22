#include "common/metric.h"

namespace kuzu {
namespace common {

TimeMetric::TimeMetric(bool enable) : Metric(enable) {
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
        throw Exception("Timer metric has not started.");
    }
    timer.stop();
    isStarted = false;
}

double TimeMetric::getElapsedTimeMS() const {
    return static_cast<double>(timer.getElapsedTimeInMS());
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

void NumericMetric::incrementByOne() {
    if (!enabled) {
        return;
    }
    accumulatedValue++;
}

} // namespace common
} // namespace kuzu
