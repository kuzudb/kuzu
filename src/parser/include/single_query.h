#pragma once

#include "src/parser/include/statements/match_statement.h"

namespace graphflow {
namespace parser {

class SingleQuery {

public:
    bool operator==(const SingleQuery& other) {
        auto result = true;
        for (auto i = 0ul; i < statements.size(); ++i) {
            result &= *statements[i] == *other.statements[i];
        }
        return result;
    }

public:
    vector<unique_ptr<MatchStatement>> statements;
};

} // namespace parser
} // namespace graphflow
