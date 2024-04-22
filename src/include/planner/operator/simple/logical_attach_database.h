#pragma once

#include "logical_simple.h"
#include "parser/parsed_data/attach_info.h"

namespace kuzu {
namespace planner {

class LogicalAttachDatabase final : public LogicalSimple {
public:
    explicit LogicalAttachDatabase(parser::AttachInfo attachInfo,
        std::shared_ptr<binder::Expression> outputExpression)
        : LogicalSimple{LogicalOperatorType::ATTACH_DATABASE, std::move(outputExpression)},
          attachInfo{std::move(attachInfo)} {}

    parser::AttachInfo getAttachInfo() const { return attachInfo; }

    std::string getExpressionsForPrinting() const override { return attachInfo.dbPath; }

    std::unique_ptr<LogicalOperator> copy() override {
        return std::make_unique<LogicalAttachDatabase>(attachInfo, outputExpression);
    }

private:
    parser::AttachInfo attachInfo;
};

} // namespace planner
} // namespace kuzu
