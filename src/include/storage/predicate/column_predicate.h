#pragma once

#include "binder/expression/expression.h"
#include "common/cast.h"
#include "common/enums/zone_map_check_result.h"

namespace kuzu {
namespace storage {

struct CompressionMetadata;

class ColumnPredicate;
class ColumnPredicateSet {
public:
    ColumnPredicateSet() = default;
    EXPLICIT_COPY_DEFAULT_MOVE(ColumnPredicateSet);

    void addPredicate(std::unique_ptr<ColumnPredicate> predicate) {
        predicates.push_back(std::move(predicate));
    }

    common::ZoneMapCheckResult checkZoneMap(const CompressionMetadata& metadata);

private:
    ColumnPredicateSet(const ColumnPredicateSet& other);

private:
    std::vector<std::unique_ptr<ColumnPredicate>> predicates;
};

class ColumnPredicate {
public:
    virtual ~ColumnPredicate() = default;

    virtual common::ZoneMapCheckResult checkZoneMap(const CompressionMetadata& metadata) const = 0;

    virtual std::unique_ptr<ColumnPredicate> copy() const = 0;

    template<class TARGET>
    const TARGET& constCast() const {
        return common::ku_dynamic_cast<const ColumnPredicate&, const TARGET&>(*this);
    }
};

struct ColumnPredicateUtil {
    static std::unique_ptr<ColumnPredicate> tryConvert(const binder::Expression& property,
        const binder::Expression& predicate);
};

} // namespace storage
} // namespace kuzu
