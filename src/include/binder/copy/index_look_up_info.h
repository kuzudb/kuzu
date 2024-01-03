#pragma once

#include "binder/expression/expression.h"
#include "common/types/types.h"

namespace kuzu {
namespace binder {

struct IndexLookupInfo {
    common::table_id_t nodeTableID;
    std::shared_ptr<binder::Expression> offset; // output
    std::shared_ptr<binder::Expression> key;    // input
    // TODO(Guodong): remove indexType. We have it because of the special code path for SERIAL.
    common::LogicalType indexType;

    IndexLookupInfo(common::table_id_t nodeTableID, std::shared_ptr<binder::Expression> offset,
        std::shared_ptr<binder::Expression> key, common::LogicalType indexType)
        : nodeTableID{nodeTableID}, offset{std::move(offset)}, key{std::move(key)},
          indexType{std::move(indexType)} {}
    EXPLICIT_COPY_DEFAULT_MOVE(IndexLookupInfo);

private:
    IndexLookupInfo(const IndexLookupInfo& other)
        : nodeTableID{other.nodeTableID}, offset{other.offset}, key{other.key},
          indexType{other.indexType} {}
};

} // namespace binder
} // namespace kuzu
