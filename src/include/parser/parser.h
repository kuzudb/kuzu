#pragma once

#include <memory>
#include <string_view>

#include "statement.h"

namespace kuzu {
namespace parser {

class Parser {

public:
    static std::unique_ptr<Statement> parseQuery(std::string_view query);
};

} // namespace parser
} // namespace kuzu
