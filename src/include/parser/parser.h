#pragma once

#include <memory>
#include <string_view>
#include <vector>

#include "statement.h"

namespace kuzu {
namespace parser {

class Parser {

public:
    static std::vector<std::shared_ptr<Statement>> parseQuery(std::string_view query);
};

} // namespace parser
} // namespace kuzu
