#pragma once

#include "binder/expression/expression.h"
#include "common/cast.h"
#include "common/enums/zone_map_check_result.h"

namespace kuzu {
namespace storage {

struct ColumnChunkStats;

class ColumnPredicate;
class KUZU_API ColumnPredicateSet {
public:
    ColumnPredicateSet() = default;
    EXPLICIT_COPY_DEFAULT_MOVE(ColumnPredicateSet);

    void addPredicate(std::unique_ptr<ColumnPredicate> predicate) {
        predicates.push_back(std::move(predicate));
    }
    bool isEmpty() const { return predicates.empty(); }

    common::ZoneMapCheckResult checkZoneMap(const ColumnChunkStats& stats) const;

    std::string toString() const;

private:
    ColumnPredicateSet(const ColumnPredicateSet& other)
        : predicates{copyVector(other.predicates)} {}

private:
    std::vector<std::unique_ptr<ColumnPredicate>> predicates;
};

class KUZU_API ColumnPredicate {
public:
    explicit ColumnPredicate(std::string columnName) : columnName{std::move(columnName)} {}

    virtual ~ColumnPredicate() = default;

    virtual common::ZoneMapCheckResult checkZoneMap(const ColumnChunkStats& stats) const = 0;

    virtual std::string toString() = 0;

    virtual std::unique_ptr<ColumnPredicate> copy() const = 0;

    template<class TARGET>
    const TARGET& constCast() const {
        return common::ku_dynamic_cast<const TARGET&>(*this);
    }

protected:
    std::string columnName;
};

struct ColumnPredicateUtil {
    static std::unique_ptr<ColumnPredicate> tryConvert(const binder::Expression& column,
        const binder::Expression& predicate);
};

} // namespace storage
} // namespace kuzu
