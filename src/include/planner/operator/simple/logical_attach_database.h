#pragma once

#include "binder/bound_attach_info.h"
#include "logical_simple.h"

namespace kuzu {
namespace planner {

class LogicalAttachDatabase final : public LogicalSimple {
public:
    explicit LogicalAttachDatabase(binder::AttachInfo attachInfo,
        std::shared_ptr<binder::Expression> outputExpression)
        : LogicalSimple{LogicalOperatorType::ATTACH_DATABASE, std::move(outputExpression)},
          attachInfo{std::move(attachInfo)} {}

    binder::AttachInfo getAttachInfo() const { return attachInfo; }

    std::string getExpressionsForPrinting() const override { return attachInfo.dbPath; }

    std::unique_ptr<LogicalOperator> copy() override {
        return std::make_unique<LogicalAttachDatabase>(attachInfo, outputExpression);
    }

private:
    binder::AttachInfo attachInfo;
};

} // namespace planner
} // namespace kuzu
