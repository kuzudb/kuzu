#pragma once

#include "query_part.h"

namespace kuzu {
namespace parser {

class SingleQuery {

public:
    SingleQuery() = default;
    ~SingleQuery() = default;

    inline void addQueryPart(std::unique_ptr<QueryPart> queryPart) {
        queryParts.push_back(std::move(queryPart));
    }
    inline uint32_t getNumQueryParts() const { return queryParts.size(); }
    inline QueryPart* getQueryPart(uint32_t idx) const { return queryParts[idx].get(); }

    inline uint32_t getNumUpdatingClauses() const { return updatingClauses.size(); }
    inline UpdatingClause* getUpdatingClause(uint32_t idx) const {
        return updatingClauses[idx].get();
    }
    inline void addUpdatingClause(std::unique_ptr<UpdatingClause> updatingClause) {
        updatingClauses.push_back(std::move(updatingClause));
    }

    inline uint32_t getNumReadingClauses() const { return readingClauses.size(); }
    inline ReadingClause* getReadingClause(uint32_t idx) const { return readingClauses[idx].get(); }
    inline void addReadingClause(std::unique_ptr<ReadingClause> readingClause) {
        readingClauses.push_back(std::move(readingClause));
    }

    inline void setReturnClause(std::unique_ptr<ReturnClause> returnClause_) {
        this->returnClause = std::move(returnClause_);
    }
    inline bool hasReturnClause() const { return returnClause != nullptr; }
    inline ReturnClause* getReturnClause() const { return returnClause.get(); }

private:
    std::vector<std::unique_ptr<QueryPart>> queryParts;
    std::vector<std::unique_ptr<ReadingClause>> readingClauses;
    std::vector<std::unique_ptr<UpdatingClause>> updatingClauses;
    std::unique_ptr<ReturnClause> returnClause;
};

} // namespace parser
} // namespace kuzu
