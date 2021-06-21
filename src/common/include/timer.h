#pragma once

#include <chrono>
#include <stdexcept>
#include <string>
#include <vector>

using namespace std;

namespace graphflow {
namespace common {

class Timer {

public:
    void start() {
        finished = false;
        startTime = chrono::high_resolution_clock::now();
    }

    void stop() {
        stopTime = chrono::high_resolution_clock::now();
        finished = true;
    }

    double getDuration() {
        if (finished) {
            auto duration = stopTime - startTime;
            return chrono::duration_cast<chrono::milliseconds>(duration).count();
        }
        throw invalid_argument("Timer is still running.");
    }

private:
    chrono::time_point<chrono::high_resolution_clock> startTime;
    chrono::time_point<chrono::high_resolution_clock> stopTime;
    bool finished = false;
};

} // namespace common
} // namespace graphflow
