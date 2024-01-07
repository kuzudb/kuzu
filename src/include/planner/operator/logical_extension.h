#pragma once

#include "extension/extension_action.h"
#include "planner/operator/logical_operator.h"

namespace kuzu {
namespace planner {

using namespace kuzu::extension;

class LogicalExtension final : public LogicalOperator {
public:
    explicit LogicalExtension(ExtensionAction action, std::string path)
        : LogicalOperator{LogicalOperatorType::EXTENSION}, action{action}, path{std::move(path)} {}

    inline std::string getExpressionsForPrinting() const override { return path; }

    inline void computeFlatSchema() override { createEmptySchema(); }
    inline void computeFactorizedSchema() override { createEmptySchema(); }

    inline ExtensionAction getAction() { return action; }
    inline std::string getPath() { return path; }

    inline std::unique_ptr<LogicalOperator> copy() override {
        return std::make_unique<LogicalExtension>(action, path);
    }

private:
    ExtensionAction action;
    std::string path;
};

} // namespace planner
} // namespace kuzu
