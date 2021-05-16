#pragma once

#include <chrono>
#include <iostream>
#include <string>
#include <vector>

using namespace std;

namespace graphflow {
namespace common {

class Timer {
public:
    explicit Timer(const string& name)
        : name{name}, stopped{false}, start{chrono::high_resolution_clock::now()} {};

    ~Timer() {}

    void stop() {
        checkpoint = chrono::high_resolution_clock::now();
        stopped = true;
    }

    chrono::milliseconds getDuration() {
        if (stopped) {
            return chrono::duration_cast<chrono::milliseconds>(checkpoint - start);
        }
        throw invalid_argument("Timer still running.");
    }

    void logCheckpoint(const string& checkpointName) {
        auto currentCheckpoint = chrono::high_resolution_clock::now();
        checkpoint = currentCheckpoint;
    }

private:
    const string name;
    bool stopped;
    chrono::time_point<chrono::high_resolution_clock> start, checkpoint;
};

} // namespace common
} // namespace graphflow
