#pragma once

#include "binder/expression/expression.h"

namespace kuzu {
namespace binder {

struct IndexLookupInfo {
    common::table_id_t nodeTableID;
    std::shared_ptr<binder::Expression> offset; // output
    std::shared_ptr<binder::Expression> key;    // input
    std::vector<std::shared_ptr<binder::Expression>> warningData;

    IndexLookupInfo(common::table_id_t nodeTableID, std::shared_ptr<binder::Expression> offset,
        std::shared_ptr<binder::Expression> key,
        std::vector<std::shared_ptr<binder::Expression>> warningData = {})
        : nodeTableID{nodeTableID}, offset{std::move(offset)}, key{std::move(key)},
          warningData(std::move(warningData)) {}
    IndexLookupInfo(const IndexLookupInfo& other) = default;
};

} // namespace binder
} // namespace kuzu
