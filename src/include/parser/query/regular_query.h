#pragma once

#include "parser/statement.h"
#include "single_query.h"

namespace kuzu {
namespace parser {

class RegularQuery : public Statement {

public:
    explicit RegularQuery(std::unique_ptr<SingleQuery> singleQuery)
        : Statement{common::StatementType::QUERY} {
        singleQueries.push_back(std::move(singleQuery));
    }

    inline void addSingleQuery(std::unique_ptr<SingleQuery> singleQuery, bool isUnionAllQuery) {
        singleQueries.push_back(std::move(singleQuery));
        isUnionAll.push_back(isUnionAllQuery);
    }

    inline uint64_t getNumSingleQueries() const { return singleQueries.size(); }

    inline SingleQuery* getSingleQuery(uint32_t singleQueryIdx) const {
        return singleQueries[singleQueryIdx].get();
    }

    inline std::vector<bool> getIsUnionAll() const { return isUnionAll; }

private:
    std::vector<std::unique_ptr<SingleQuery>> singleQueries;
    std::vector<bool> isUnionAll;
};

} // namespace parser
} // namespace kuzu
