#pragma once

#include "binder/expression/expression.h"
namespace kuzu {
namespace binder {

struct IndexLookupInfo {
    common::table_id_t nodeTableID;
    std::shared_ptr<binder::Expression> offset; // output
    std::shared_ptr<binder::Expression> key;    // input

    IndexLookupInfo(common::table_id_t nodeTableID, std::shared_ptr<binder::Expression> offset,
        std::shared_ptr<binder::Expression> key)
        : nodeTableID{nodeTableID}, offset{std::move(offset)}, key{std::move(key)} {}
    EXPLICIT_COPY_DEFAULT_MOVE(IndexLookupInfo);

private:
    IndexLookupInfo(const IndexLookupInfo& other)
        : nodeTableID{other.nodeTableID}, offset{other.offset}, key{other.key} {}
};

} // namespace binder
} // namespace kuzu
