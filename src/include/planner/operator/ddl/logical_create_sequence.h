#pragma once

#include "binder/ddl/bound_create_sequence_info.h"
#include "planner/operator/ddl/logical_ddl.h"

namespace kuzu {
namespace planner {

class LogicalCreateSequence : public LogicalDDL {
public:
    LogicalCreateSequence(std::string sequenceName, binder::BoundCreateSequenceInfo info,
        std::shared_ptr<binder::Expression> outputExpression)
        : LogicalDDL{LogicalOperatorType::CREATE_SEQUENCE, std::move(sequenceName),
              std::move(outputExpression)},
          info{std::move(info)} {}

    binder::BoundCreateSequenceInfo getInfo() const { return info.copy(); }

    inline std::unique_ptr<LogicalOperator> copy() final {
        return std::make_unique<LogicalCreateSequence>(tableName, info.copy(), outputExpression);
    }

private:
    binder::BoundCreateSequenceInfo info;
};

} // namespace planner
} // namespace kuzu
