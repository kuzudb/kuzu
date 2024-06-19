#pragma once

#include "planner/operator/ddl/logical_ddl.h"

namespace kuzu {
namespace planner {

class LogicalCreateType : public LogicalDDL {
    static constexpr LogicalOperatorType type_ = LogicalOperatorType::CREATE_TYPE;

public:
    LogicalCreateType(std::string name, common::LogicalType type,
        std::shared_ptr<binder::Expression> outputExpression)
        : LogicalDDL{type_, std::move(name), std::move(outputExpression)}, type{std::move(type)} {}

    const common::LogicalType& getType() const { return type; }

    inline std::unique_ptr<LogicalOperator> copy() final {
        return std::make_unique<LogicalCreateType>(tableName, type.copy(), outputExpression);
    }

private:
    common::LogicalType type;
};

} // namespace planner
} // namespace kuzu
