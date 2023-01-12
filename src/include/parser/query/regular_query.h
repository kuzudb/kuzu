#pragma once

#include "parser/statement.h"
#include "single_query.h"

namespace kuzu {
namespace parser {

class RegularQuery : public Statement {

public:
    explicit RegularQuery(unique_ptr<SingleQuery> singleQuery) : Statement{StatementType::QUERY} {
        singleQueries.push_back(move(singleQuery));
    }

    inline void addSingleQuery(unique_ptr<SingleQuery> singleQuery, bool isUnionAllQuery) {
        singleQueries.push_back(move(singleQuery));
        isUnionAll.push_back(isUnionAllQuery);
    }

    inline uint64_t getNumSingleQueries() const { return singleQueries.size(); }

    inline SingleQuery* getSingleQuery(uint32_t singleQueryIdx) const {
        return singleQueries[singleQueryIdx].get();
    }

    inline vector<bool> getIsUnionAll() const { return isUnionAll; }

private:
    vector<unique_ptr<SingleQuery>> singleQueries;
    vector<bool> isUnionAll;
};

} // namespace parser
} // namespace kuzu
