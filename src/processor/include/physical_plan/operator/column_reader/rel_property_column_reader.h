#pragma once

#include "src/processor/include/physical_plan/operator/column_reader/column_reader.h"

namespace graphflow {
namespace processor {

class RelPropertyColumnReader : public ColumnReader {

public:
    RelPropertyColumnReader(const uint64_t& dataChunkPos, const uint64_t& valueVectorPos,
        BaseColumn* column, unique_ptr<PhysicalOperator> prevOperator);

    unique_ptr<PhysicalOperator> clone() override {
        return make_unique<RelPropertyColumnReader>(
            dataChunkPos, valueVectorPos, column, prevOperator->clone());
    }
};

} // namespace processor
} // namespace graphflow
