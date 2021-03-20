#pragma once

#include <exception>
#include <memory>

#include "src/processor/include/physical_plan/operator/tuple/data_chunks.h"
#include "src/processor/include/task_system/morsel.h"
#include "src/storage/include/graph.h"

using namespace std;
using namespace graphflow::storage;

namespace graphflow {
namespace processor {

class PhysicalOperator {

public:
    PhysicalOperator() : logger{spdlog::get("processor")} {};

    PhysicalOperator(unique_ptr<PhysicalOperator> prevOperator) : PhysicalOperator{} {
        this->prevOperator.reset(prevOperator.release());
    }

    virtual ~PhysicalOperator() = default;

    virtual bool hasNextMorsel() { return prevOperator->hasNextMorsel(); };

    // Warning: getNextTuples() should only be called if getNextMorsel() returns true.
    virtual void getNextTuples() = 0;

    shared_ptr<DataChunks> getDataChunks() { return dataChunks; };

    virtual unique_ptr<PhysicalOperator> clone() = 0;

protected:
    shared_ptr<spdlog::logger> logger;
    shared_ptr<DataChunks> dataChunks;
    unique_ptr<PhysicalOperator> prevOperator;
};

} // namespace processor
} // namespace graphflow
