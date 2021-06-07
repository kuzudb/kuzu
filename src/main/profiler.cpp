#include "src/main/include/profiler.h"

#include <utility>

namespace graphflow {
namespace main {

TimeMetric::TimeMetric(string name, bool enable) : name{move(name)}, enable{enable} {
    accumulatedTime = 0;
    isStarted = false;
    timer = Timer();
}

void TimeMetric::start() {
    if (!enable) {
        return;
    }
    isStarted = true;
    timer.start();
}

void TimeMetric::stop() {
    if (!enable) {
        return;
    }
    if (!isStarted) {
        throw invalid_argument("Timer metric has not started.");
    }
    timer.stop();
    accumulatedTime += timer.getDuration();
    isStarted = false;
}

TimeMetric* Profiler::registerPhaseTimeMetric(const string& phase) {
    phaseTimeMetrics.emplace_back(make_unique<TimeMetric>(phase, enable));
    return phaseTimeMetrics[phaseTimeMetrics.size() - 1].get();
}

void Profiler::reset() {
    phaseTimeMetrics.clear();
}

nlohmann::json Profiler::toJson() {
    auto json = nlohmann::json();
    if (!enable) {
        return json;
    }
    json["phaseTimes"] = {};
    for (auto& timeMetric : phaseTimeMetrics) {
        json["phaseTimes"][timeMetric->name] = to_string(timeMetric->accumulatedTime) + " ms";
    }
    return json;
}

} // namespace main
} // namespace graphflow
