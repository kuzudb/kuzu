#pragma once

#include "bound_statement.h"
#include "extension/extension_action.h"

namespace kuzu {
namespace binder {

using namespace kuzu::extension;

class BoundExtensionStatement final : public BoundStatement {
public:
    explicit BoundExtensionStatement(ExtensionAction action, std::string path)
        : BoundStatement{common::StatementType::EXTENSION,
              BoundStatementResult::createSingleStringColumnResult()},
          action{action}, path{std::move(path)} {}

    inline ExtensionAction getAction() const { return action; }

    inline std::string getPath() const { return path; }

private:
    ExtensionAction action;
    std::string path;
};

} // namespace binder
} // namespace kuzu
