#pragma once

#include <string>

#include "parser/statement.h"

namespace kuzu {
namespace parser {

class Drop : public Statement {
public:
    explicit Drop(common::StatementType type, std::string name)
        : Statement{type}, name{std::move(name)} {}

    inline std::string getName() const { return name; }

private:
    std::string name;
};

} // namespace parser
} // namespace kuzu
