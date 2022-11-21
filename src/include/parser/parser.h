#pragma once

#include <memory>
#include <string>

#include "statement.h"

namespace kuzu {
namespace parser {

using namespace std;

class Parser {

public:
    static unique_ptr<Statement> parseQuery(const string& query);
};

} // namespace parser
} // namespace kuzu
