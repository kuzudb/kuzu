#pragma once

#include "storage/table/node_group_collection.h"

namespace kuzu {
namespace storage {

class LocalOptimisticWriter {
public:
    LocalOptimisticWriter(const std::vector<common::LogicalType>& types, bool enableCompression)
        : nodeGroups{types, enableCompression} {}

private:
    NodeGroupCollection nodeGroups;
};

} // namespace storage
} // namespace kuzu
