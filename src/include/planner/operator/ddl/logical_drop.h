#pragma once

#include "parser/ddl/drop_info.h"
#include "planner/operator/simple/logical_simple.h"

namespace kuzu {
namespace planner {

struct LogicalDropPrintInfo : OPPrintInfo {
    std::string name;

    explicit LogicalDropPrintInfo(std::string name) : name{std::move(name)} {}

    std::string toString() const override { return name; }
};

class LogicalDrop : public LogicalSimple {
    static constexpr LogicalOperatorType type_ = LogicalOperatorType::DROP;

public:
    LogicalDrop(parser::DropInfo dropInfo, std::shared_ptr<binder::Expression> outputExpression)
        : LogicalSimple{type_, std::move(outputExpression)}, dropInfo{std::move(dropInfo)} {}

    std::string getExpressionsForPrinting() const override { return dropInfo.name; }

    const parser::DropInfo& getDropInfo() const { return dropInfo; }

    std::unique_ptr<OPPrintInfo> getPrintInfo() const override {
        return std::make_unique<LogicalDropPrintInfo>(dropInfo.name);
    }

    std::unique_ptr<LogicalOperator> copy() override {
        return make_unique<LogicalDrop>(dropInfo, outputExpression);
    }

private:
    parser::DropInfo dropInfo;
};

} // namespace planner
} // namespace kuzu
