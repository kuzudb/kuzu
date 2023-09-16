#pragma once

#include "processor/operator/physical_operator.h"

namespace arrow {
class Array;
class PrimitiveArray;
class DataType;
} // namespace arrow

namespace kuzu {
namespace storage {
class PrimaryKeyIndex;
}
namespace processor {

struct IndexLookupInfo {
    std::unique_ptr<common::LogicalType> pkDataType;
    storage::PrimaryKeyIndex* pkIndex; // NULL if the PK data type is SERIAL.
    DataPos keyVectorPos;
    DataPos resultVectorPos;

    IndexLookupInfo(std::unique_ptr<common::LogicalType> pkDataType,
        storage::PrimaryKeyIndex* pkIndex, const DataPos& keyVectorPos,
        const DataPos& resultVectorPos)
        : pkDataType{std::move(pkDataType)}, pkIndex{pkIndex}, keyVectorPos{keyVectorPos},
          resultVectorPos{resultVectorPos} {}

    inline std::unique_ptr<IndexLookupInfo> copy() {
        return std::make_unique<IndexLookupInfo>(
            pkDataType->copy(), pkIndex, keyVectorPos, resultVectorPos);
    }
};

class IndexLookup : public PhysicalOperator {
public:
    IndexLookup(std::vector<std::unique_ptr<IndexLookupInfo>> infos,
        std::unique_ptr<PhysicalOperator> child, uint32_t id, const std::string& paramsString)
        : PhysicalOperator{PhysicalOperatorType::INDEX_LOOKUP, std::move(child), id, paramsString},
          infos{std::move(infos)} {}

    bool getNextTuplesInternal(ExecutionContext* context) final;

    std::unique_ptr<PhysicalOperator> clone() final;

private:
    void indexLookup(const IndexLookupInfo& info);
    static void lookupOnArray(
        const IndexLookupInfo& info, arrow::Array* array, common::offset_t* offsets);
    void fillOffsetArraysFromArrowVector(const IndexLookupInfo& info,
        common::ValueVector* keyVector,
        std::vector<std::shared_ptr<arrow::Array>>& offsetArraysVector);
    void fillOffsetArraysFromVector(const IndexLookupInfo& info, common::ValueVector* keyVector,
        std::vector<std::shared_ptr<arrow::Array>>& offsetArraysVector);

private:
    std::vector<std::unique_ptr<IndexLookupInfo>> infos;
};

} // namespace processor
} // namespace kuzu
