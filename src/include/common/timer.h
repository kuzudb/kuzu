#pragma once

#include <chrono>
#include <string>

#include "exception/exception.h"

namespace kuzu {
namespace common {

class Timer {

public:
    void start() {
        finished = false;
        startTime = std::chrono::high_resolution_clock::now();
    }

    void stop() {
        stopTime = std::chrono::high_resolution_clock::now();
        finished = true;
    }

    double getDuration() {
        if (finished) {
            auto duration = stopTime - startTime;
            return (double)std::chrono::duration_cast<std::chrono::microseconds>(duration).count();
        }
        throw Exception("Timer is still running.");
    }

    int64_t getElapsedTimeInMS() {
        auto now = std::chrono::high_resolution_clock::now();
        auto duration = now - startTime;
        return std::chrono::duration_cast<std::chrono::milliseconds>(duration).count();
    }

private:
    std::chrono::time_point<std::chrono::high_resolution_clock> startTime;
    std::chrono::time_point<std::chrono::high_resolution_clock> stopTime;
    bool finished = false;
};

} // namespace common
} // namespace kuzu
