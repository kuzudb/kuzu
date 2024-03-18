#pragma once

#include "parser/parsed_data/attach_info.h"
#include "planner/operator/logical_operator.h"

namespace kuzu {
namespace planner {

class LogicalAttachDatabase final : public LogicalOperator {
public:
    explicit LogicalAttachDatabase(parser::AttachInfo attachInfo)
        : LogicalOperator{LogicalOperatorType::ATTACH_DATABASE}, attachInfo{std::move(attachInfo)} {
    }

    parser::AttachInfo getAttachInfo() const { return attachInfo; }

    std::string getExpressionsForPrinting() const override { return attachInfo.dbPath; }

    void computeFactorizedSchema() override { createEmptySchema(); }
    void computeFlatSchema() override { createEmptySchema(); }

    std::unique_ptr<LogicalOperator> copy() override {
        return std::make_unique<LogicalAttachDatabase>(attachInfo);
    }

private:
    parser::AttachInfo attachInfo;
};

} // namespace planner
} // namespace kuzu
