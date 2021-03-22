#pragma once

#include <cstdint>

namespace graphflow {
namespace processor {

class PlanOutput {

public:
    PlanOutput(const uint64_t& numOutputTuples) : numOutputTuples(numOutputTuples) {}
    PlanOutput() : PlanOutput(0){};

    uint64_t getNumOutputTuples() { return numOutputTuples; }

    static void aggregate(vector<unique_ptr<PlanOutput>>& threadOutputs, PlanOutput& result) {
        result.numOutputTuples = 0;
        for (auto& threadOutput : threadOutputs) {
            result.numOutputTuples += threadOutput->getNumOutputTuples();
        }
    }

protected:
    uint64_t numOutputTuples = 0;
};

} // namespace processor
} // namespace graphflow
