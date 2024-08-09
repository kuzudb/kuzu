#pragma once

#include "parser/ddl/drop_info.h"
#include "planner/operator/ddl/logical_ddl.h"
#include "planner/operator/logical_operator.h"

namespace kuzu {
namespace planner {

class LogicalDrop : public LogicalDDL {
public:
    LogicalDrop(const parser::DropInfo& dropInfo,
        std::shared_ptr<binder::Expression> outputExpression,
        std::unique_ptr<OPPrintInfo> printInfo)
        : LogicalDDL{LogicalOperatorType::DROP, dropInfo.name, std::move(outputExpression),
              std::move(printInfo)},
          dropInfo{dropInfo} {}

    const parser::DropInfo& getDropInfo() const { return dropInfo; }

    std::unique_ptr<LogicalOperator> copy() override {
        return make_unique<LogicalDrop>(dropInfo, outputExpression, printInfo->copy());
    }

private:
    parser::DropInfo dropInfo;
};

} // namespace planner
} // namespace kuzu
