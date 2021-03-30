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

// Physical operator type
enum PhysicalOperatorType : uint8_t {
    SCAN,
    FILTER,
    FLATTEN,
    HASH_JOIN_BUILD,
    HASH_JOIN_PROBE,
    COLUMN_READER,
    LIST_READER,
    RESULT_COLLECTOR,
};

class PhysicalOperator {

public:
    explicit PhysicalOperator(PhysicalOperatorType operatorType)
        : PhysicalOperator(nullptr, operatorType, false) {}

    PhysicalOperator(unique_ptr<PhysicalOperator> prevOperator, PhysicalOperatorType operatorType)
        : PhysicalOperator(move(prevOperator), operatorType, false) {}

    PhysicalOperator(unique_ptr<PhysicalOperator> prevOperator, PhysicalOperatorType operatorType,
        bool isOutDataChunkFiltered)
        : prevOperator{move(prevOperator)}, operatorType{operatorType},
          isOutDataChunkFiltered{isOutDataChunkFiltered}, logger{spdlog::get("processor")} {}

    virtual ~PhysicalOperator() = default;

    virtual bool hasNextMorsel() { return prevOperator->hasNextMorsel(); };

    // Warning: getNextTuples() should only be called if getNextMorsel() returns true.
    virtual void getNextTuples() = 0;

    shared_ptr<DataChunks> getDataChunks() { return dataChunks; };

    virtual unique_ptr<PhysicalOperator> clone() = 0;

public:
    unique_ptr<PhysicalOperator> prevOperator;
    PhysicalOperatorType operatorType;
    bool isOutDataChunkFiltered;

protected:
    shared_ptr<spdlog::logger> logger;
    shared_ptr<DataChunks> dataChunks;
};

} // namespace processor
} // namespace graphflow
