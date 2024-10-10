#pragma once

#include "parser/ddl/drop_info.h"
#include "planner/operator/ddl/logical_ddl.h"
#include "planner/operator/logical_operator.h"

namespace kuzu {
namespace planner {

struct LogicalDropPrintInfo final : OPPrintInfo {
    std::string name;

    explicit LogicalDropPrintInfo(std::string name) : name{std::move(name)} {}

    std::string toString() const override { return "Drop: " + name; }

    std::unique_ptr<OPPrintInfo> copy() const override {
        return std::make_unique<LogicalDropPrintInfo>(name);
    }
};

class LogicalDrop final : public LogicalDDL {
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
