#pragma once

#include "src/common/include/timer.h"

using namespace std;

namespace graphflow {
namespace common {

class Metric {

public:
    explicit Metric(bool enabled) : enabled{enabled} {}

public:
    bool enabled;
};

class TimeMetric : public Metric {

public:
    explicit TimeMetric(bool enable);

    void start();
    void stop();

public:
    double accumulatedTime;
    bool isStarted;
    Timer timer;
};

class NumericMetric : public Metric {

public:
    explicit NumericMetric(bool enable);

    void increase(uint64_t value);

public:
    uint64_t accumulatedValue;
};

} // namespace common
} // namespace graphflow
