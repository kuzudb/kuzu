#pragma once

#include "planner/operator/ddl/logical_ddl.h"
#include "planner/operator/logical_operator.h"

namespace kuzu {
namespace planner {

class LogicalDropSequence : public LogicalDDL {
public:
    LogicalDropSequence(common::sequence_id_t sequenceID, std::string name,
        std::shared_ptr<binder::Expression> outputExpression)
        : LogicalDDL{LogicalOperatorType::DROP_SEQUENCE, std::move(name),
              std::move(outputExpression)},
          sequenceID{sequenceID} {}

    inline common::sequence_id_t getSequenceID() const { return sequenceID; }

    inline std::unique_ptr<LogicalOperator> copy() override {
        return make_unique<LogicalDropSequence>(sequenceID, tableName, outputExpression);
    }

private:
    common::sequence_id_t sequenceID;
};

} // namespace planner
} // namespace kuzu
