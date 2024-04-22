#pragma once

#include "extension/extension_action.h"
#include "logical_simple.h"

namespace kuzu {
namespace planner {

using namespace kuzu::extension;

class LogicalExtension final : public LogicalSimple {
public:
    explicit LogicalExtension(ExtensionAction action, std::string path,
        std::shared_ptr<binder::Expression> outputExpression)
        : LogicalSimple{LogicalOperatorType::EXTENSION, std::move(outputExpression)},
          action{action}, path{std::move(path)} {}

    std::string getExpressionsForPrinting() const override { return path; }

    ExtensionAction getAction() { return action; }
    std::string getPath() { return path; }

    std::unique_ptr<LogicalOperator> copy() override {
        return std::make_unique<LogicalExtension>(action, path, outputExpression);
    }

private:
    ExtensionAction action;
    std::string path;
};

} // namespace planner
} // namespace kuzu
