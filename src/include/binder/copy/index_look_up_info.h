#pragma once

#include "binder/expression/expression.h"

namespace kuzu {
namespace binder {

struct IndexLookupInfo {
    common::table_id_t nodeTableID;
    std::shared_ptr<binder::Expression> offset; // output
    std::shared_ptr<binder::Expression> key;    // input
    binder::expression_vector warningExprs;

    IndexLookupInfo(common::table_id_t nodeTableID, std::shared_ptr<binder::Expression> offset,
        std::shared_ptr<binder::Expression> key, binder::expression_vector warningExprs = {})
        : nodeTableID{nodeTableID}, offset{std::move(offset)}, key{std::move(key)},
          warningExprs(std::move(warningExprs)) {}
    IndexLookupInfo(const IndexLookupInfo& other) = default;
};

} // namespace binder
} // namespace kuzu
