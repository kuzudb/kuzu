#pragma once

#include <memory>
#include <string>

#include "statement.h"

namespace kuzu {
namespace parser {

class Parser {

public:
    static std::unique_ptr<Statement> parseQuery(const std::string& query);
};

} // namespace parser
} // namespace kuzu
