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
    std::unique_ptr<common::LogicalType> indexType;

    IndexLookupInfo(common::table_id_t nodeTableID, std::shared_ptr<binder::Expression> offset,
        std::shared_ptr<binder::Expression> key, std::unique_ptr<common::LogicalType> indexType)
        : nodeTableID{nodeTableID}, offset{std::move(offset)}, key{std::move(key)},
          indexType{std::move(indexType)} {}
    IndexLookupInfo(const IndexLookupInfo& other)
        : nodeTableID{other.nodeTableID}, offset{other.offset}, key{other.key},
          indexType{other.indexType->copy()} {}

    inline std::unique_ptr<IndexLookupInfo> copy() const {
        return std::make_unique<IndexLookupInfo>(*this);
    }

    static std::vector<std::unique_ptr<IndexLookupInfo>> copy(
        const std::vector<std::unique_ptr<IndexLookupInfo>>& infos) {
        std::vector<std::unique_ptr<IndexLookupInfo>> result;
        result.reserve(infos.size());
        for (auto& info : infos) {
            result.push_back(info->copy());
        }
        return result;
    }
};

} // namespace binder
} // namespace kuzu
