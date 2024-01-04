#pragma once

#include <string>

#include "statement.h"

namespace kuzu {
namespace parser {

class LoadExtensionStatement final : public Statement {
public:
    explicit LoadExtensionStatement(std::string path)
        : Statement{common::StatementType::LOAD_EXTENSION}, path{std::move(path)} {}

    inline std::string getPath() const { return path; }

private:
    std::string path;
};

} // namespace parser
} // namespace kuzu
