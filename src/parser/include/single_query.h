#pragma once

#include "src/parser/include/statements/match_statement.h"
#include "src/parser/include/statements/return_statement.h"

namespace graphflow {
namespace parser {

class SingleQuery {

public:
    bool operator==(const SingleQuery& other) {
        auto result = matchStatements.size() == other.matchStatements.size() &&
                      *returnStatement == *other.returnStatement;
        if (result) {
            for (auto i = 0u; i < matchStatements.size(); ++i) {
                if (!(*matchStatements[i] == *other.matchStatements[i])) {
                    return false;
                }
            }
            return true;
        }
        return false;
    }

public:
    vector<unique_ptr<MatchStatement>> matchStatements;
    unique_ptr<ReturnStatement> returnStatement;
};

} // namespace parser
} // namespace graphflow
