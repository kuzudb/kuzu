#pragma once

#include "updating_clause.h"

#include "src/parser/expression/include/parsed_expression.h"
#include "src/parser/query/match_clause/include/node_pattern.h"

namespace graphflow {
namespace parser {

class CreateClause : public UpdatingClause {
public:
    CreateClause() : UpdatingClause{ClauseType::CREATE} {};
    ~CreateClause() override = default;

    inline void addNodePattern(unique_ptr<NodePattern> nodePattern) {
        nodePatterns.push_back(move(nodePattern));
    }
    inline uint32_t getNumNodePatterns() const { return nodePatterns.size(); }
    inline NodePattern* getNodePattern(uint32_t idx) const { return nodePatterns[idx].get(); }

private:
    vector<unique_ptr<NodePattern>> nodePatterns;
};

} // namespace parser
} // namespace graphflow
