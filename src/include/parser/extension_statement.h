#pragma once

#include <string>

#include "extension/extension_action.h"
#include "statement.h"

namespace kuzu {
namespace parser {

using namespace kuzu::extension;

class ExtensionStatement final : public Statement {
public:
    explicit ExtensionStatement(ExtensionAction action, std::string path)
        : Statement{common::StatementType::EXTENSION}, action{action}, path{std::move(path)} {}

    inline ExtensionAction getAction() const { return action; }

    inline std::string getPath() const { return path; }

private:
    ExtensionAction action;
    std::string path;
};

} // namespace parser
} // namespace kuzu
