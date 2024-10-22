#pragma once

#include "planner/operator/ddl/logical_ddl.h"

namespace kuzu {
namespace planner {

struct LogicalCreateTypePrintInfo final : OPPrintInfo {
    std::string typeName;
    std::string type;

    LogicalCreateTypePrintInfo(std::string typeName, std::string type)
        : typeName(std::move(typeName)), type(std::move(type)) {}

    std::string toString() const override { return typeName + " As " + type; };

    std::unique_ptr<OPPrintInfo> copy() const override {
        return std::unique_ptr<LogicalCreateTypePrintInfo>(new LogicalCreateTypePrintInfo(*this));
    }

private:
    LogicalCreateTypePrintInfo(const LogicalCreateTypePrintInfo& other)
        : OPPrintInfo(other), typeName(other.typeName), type(other.type) {}
};

class LogicalCreateType : public LogicalDDL {
    static constexpr LogicalOperatorType type_ = LogicalOperatorType::CREATE_TYPE;

public:
    LogicalCreateType(std::string name, common::LogicalType type,
        std::shared_ptr<binder::Expression> outputExpression)
        : LogicalDDL{type_, std::move(name), std::move(outputExpression)}, type{std::move(type)} {}

    const common::LogicalType& getType() const { return type; }

    std::unique_ptr<OPPrintInfo> getPrintInfo() const override {
        return std::make_unique<LogicalCreateTypePrintInfo>(tableName, type.toString());
    }

    inline std::unique_ptr<LogicalOperator> copy() final {
        return std::make_unique<LogicalCreateType>(tableName, type.copy(), outputExpression);
    }

private:
    common::LogicalType type;
};

} // namespace planner
} // namespace kuzu
