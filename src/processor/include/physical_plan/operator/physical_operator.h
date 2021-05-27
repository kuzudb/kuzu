#pragma once

#include <exception>
#include <memory>

#include "src/processor/include/physical_plan/operator/result/result_set.h"
#include "src/processor/include/task_system/morsel.h"
#include "src/storage/include/graph.h"

using namespace std;

namespace graphflow {
namespace processor {

// Physical operator type
enum PhysicalOperatorType : uint8_t {
    SCAN,
    FILTER,
    FLATTEN,
    READ_LIST,
    SCAN_ATTRIBUTE,
    PROJECTION,
    FRONTIER_EXTEND,
    HASH_JOIN_BUILD,
    HASH_JOIN_PROBE,
    RESULT_COLLECTOR,
    LOAD_CSV,
    CREATE_NODE,
    UPDATE_NODE,
    DELETE_NODE
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

    virtual void getNextTuples() = 0;

    shared_ptr<ResultSet> getResultSet() { return resultSet; };

    virtual unique_ptr<PhysicalOperator> clone() = 0;

public:
    unique_ptr<PhysicalOperator> prevOperator;
    PhysicalOperatorType operatorType;
    bool isOutDataChunkFiltered;

protected:
    shared_ptr<spdlog::logger> logger;
    shared_ptr<ResultSet> resultSet;
};

} // namespace processor
} // namespace graphflow
