#pragma once

#include "parser/statement.h"
#include "single_query.h"

namespace kuzu {
namespace parser {

class RegularQuery : public Statement {
public:
    explicit RegularQuery(SingleQuery singleQuery) : Statement{common::StatementType::QUERY} {
        singleQueries.push_back(std::move(singleQuery));
    }

    inline void addSingleQuery(SingleQuery singleQuery, bool isUnionAllQuery) {
        singleQueries.push_back(std::move(singleQuery));
        isUnionAll.push_back(isUnionAllQuery);
    }

    inline uint64_t getNumSingleQueries() const { return singleQueries.size(); }

    inline const SingleQuery* getSingleQuery(uint32_t singleQueryIdx) const {
        return &singleQueries[singleQueryIdx];
    }

    inline std::vector<bool> getIsUnionAll() const { return isUnionAll; }

private:
    std::vector<SingleQuery> singleQueries;
    std::vector<bool> isUnionAll;
};

} // namespace parser
} // namespace kuzu
