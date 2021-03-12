#pragma once

#include "src/parser/include/statements/match_statement.h"

namespace graphflow {
namespace parser {

class SingleQuery {

public:
    void addMatchStatement(unique_ptr<MatchStatement> statement) {
        statements.push_back(move(statement));
    }

    bool operator==(const SingleQuery& other) {
        auto result = true;
        for (auto i = 0ul; i < statements.size(); ++i) {
            result &= *statements[i] == *other.statements[i];
        }
        return result;
    }

private:
    vector<unique_ptr<MatchStatement>> statements;
};

} // namespace parser
} // namespace graphflow
