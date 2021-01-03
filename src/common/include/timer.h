#pragma once

#include <chrono>
#include <iostream>
#include <string>
#include <vector>

#include <spdlog/sinks/stdout_sinks.h>
#include <spdlog/spdlog.h>

using namespace std;

namespace graphflow {
namespace common {

class Timer {
public:
    Timer(const string& name)
        : logger{spdlog::stdout_logger_st("timer")}, name{name},
          start{chrono::high_resolution_clock::now()} {
        Timer::logger->info("{} started.", name);
    };

    ~Timer() { spdlog::drop("timer"); }

    void stop() {
        checkpoint = chrono::high_resolution_clock::now();
        stopped = true;
    }

    chrono::milliseconds getDuration() {
        if (stopped) {
            return duration_cast<chrono::milliseconds>(checkpoint - start);
        }
        throw new invalid_argument("Timer still running.");
    }

    void logCheckpoint(const string& checkpointName) {
        auto currentCheckpoint = chrono::high_resolution_clock::now();
        Timer::logger->info(
            "Checkpoint:{}. Time since last checkpoint {} ms. Time since start {} ms.", name,
            duration_cast<chrono::milliseconds>(currentCheckpoint - checkpoint).count(),
            duration_cast<chrono::milliseconds>(currentCheckpoint - start).count());
        checkpoint = currentCheckpoint;
    }

private:
    shared_ptr<spdlog::logger> logger;
    const string name;
    bool stopped{false};
    chrono::time_point<chrono::high_resolution_clock> start, checkpoint;
};

} // namespace common
} // namespace graphflow
