#pragma once

#include "src/parser/include/queries/single_query.h"

namespace graphflow {
namespace parser {

class RegularQuery {

public:
    inline void addSingleQuery(unique_ptr<SingleQuery> singleQuery) {
        singleQueries.push_back(move(singleQuery));
    }

    inline SingleQuery* getSingleQuery(uint32_t singleQueryIdx) const {
        return singleQueries[singleQueryIdx].get();
    }

    inline uint64_t getNumSingleQueries() const { return singleQueries.size(); }

    inline void setEnableExplain(bool option) { enable_explain = option; }

    inline void setEnableProfile(bool option) { enable_profile = option; }

    inline bool isEnableExplain() const { return enable_explain; }

    inline bool isEnableProfile() const { return enable_profile; }

private:
    vector<unique_ptr<SingleQuery>> singleQueries;
    // If explain is enabled, we do not execute query but return physical plan only.
    bool enable_explain = false;
    bool enable_profile = false;
};

} // namespace parser
} // namespace graphflow
