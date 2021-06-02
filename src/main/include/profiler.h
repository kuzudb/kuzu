#pragma once

#include <memory>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "nlohmann/json.hpp"

#include "src/common/include/timer.h"

using namespace graphflow::common;
using namespace std;

namespace graphflow {
namespace main {

class TimeMetric {

public:
    TimeMetric(string name, bool enable);

    void start();
    void stop();

public:
    string name;
    bool enable;
    double accumulatedTime;
    bool isStarted;
    Timer timer;
};

/**
 * Each query profiler is associated with a submitted query.
 * Note that Profiler is not thread safe, multiple threads cannot write to the same profiler object
 */
class Profiler {

public:
    TimeMetric* registerPhaseTimeMetric(const string& phase);

    void reset();

    nlohmann::json toJson();

public:
    bool enable = false;
    // time spent in binding, planning and executing
    vector<unique_ptr<TimeMetric>> phaseTimeMetrics;
};

} // namespace main
} // namespace graphflow
