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

    inline void setEnableExplain(bool option) { enable_explain = option; }

    inline bool isEnableExplain() const { return enable_explain; }

    inline void setEnableProfile(bool option) { enable_profile = option; }

    inline bool isEnableProfile() const { return enable_profile; }

private:
    vector<unique_ptr<SingleQuery>> singleQueries;
    vector<bool> isUnionAll;
    // If explain is enabled, we do not execute query but return physical plan only.
    bool enable_explain = false;
    bool enable_profile = false;
};

} // namespace parser
} // namespace kuzu
