#pragma once

#include "common/types/types.h"

namespace kuzu {
namespace planner {
class Schema;
} // namespace planner

namespace processor {

struct DataChunkDescriptor {
    bool isSingleState;
    std::vector<common::LogicalType> logicalTypes;

    explicit DataChunkDescriptor(bool isSingleState) : isSingleState{isSingleState} {}
    DataChunkDescriptor(const DataChunkDescriptor& other)
        : isSingleState{other.isSingleState}, logicalTypes(other.logicalTypes) {}

    inline std::unique_ptr<DataChunkDescriptor> copy() const {
        return std::make_unique<DataChunkDescriptor>(*this);
    }
};

struct ResultSetDescriptor {
    std::vector<std::unique_ptr<DataChunkDescriptor>> dataChunkDescriptors;

    explicit ResultSetDescriptor(
        std::vector<std::unique_ptr<DataChunkDescriptor>> dataChunkDescriptors)
        : dataChunkDescriptors{std::move(dataChunkDescriptors)} {}
    explicit ResultSetDescriptor(planner::Schema* schema);

    std::unique_ptr<ResultSetDescriptor> copy() const;
};

} // namespace processor
} // namespace kuzu
