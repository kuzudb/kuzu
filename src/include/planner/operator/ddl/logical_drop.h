#pragma once

#include "parser/ddl/drop_info.h"
#include "planner/operator/ddl/logical_ddl.h"
#include "planner/operator/logical_operator.h"

namespace kuzu {
namespace planner {

struct LogicalDropPrintInfo : OPPrintInfo {
    std::string name;

    explicit LogicalDropPrintInfo(std::string name) : name{std::move(name)} {}

    std::string toString() const override { return name; }
};

class LogicalDrop : public LogicalDDL {
public:
    LogicalDrop(const parser::DropInfo& dropInfo,
        std::shared_ptr<binder::Expression> outputExpression)
        : LogicalDDL{LogicalOperatorType::DROP, dropInfo.name, std::move(outputExpression)},
          dropInfo{dropInfo} {}

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
