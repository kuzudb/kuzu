#pragma once

#include <memory>
#include <string_view>
#include <vector>

#include "statement.h"

namespace kuzu {
namespace main {
class Database;
}

namespace parser {

class Parser {

public:
    explicit Parser(main::Database* database) : database{database} {}

    std::vector<std::shared_ptr<Statement>> parseQuery(std::string_view query) const;

private:
    main::Database* database;
};

} // namespace parser
} // namespace kuzu
