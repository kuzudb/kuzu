#pragma once

#include "processor/operator/physical_operator.h"

namespace kuzu {
namespace storage {
class PrimaryKeyIndex;
} // namespace storage
namespace processor {

struct BatchInsertSharedState;
struct IndexLookupInfo {
    std::unique_ptr<common::LogicalType> pkDataType;
    storage::PrimaryKeyIndex* index; // NULL if the PK data type is SERIAL.
    // In copy rdf, we need to perform lookup before primary key is persist on disk. So we need to
    // use index builder.
    std::shared_ptr<BatchInsertSharedState> batchInsertSharedState;
    DataPos keyVectorPos;
    DataPos resultVectorPos;

    IndexLookupInfo(std::unique_ptr<common::LogicalType> pkDataType,
        storage::PrimaryKeyIndex* index, const DataPos& keyVectorPos,
        const DataPos& resultVectorPos)
        : pkDataType{std::move(pkDataType)}, index{index}, batchInsertSharedState{nullptr},
          keyVectorPos{keyVectorPos}, resultVectorPos{resultVectorPos} {}
    IndexLookupInfo(const IndexLookupInfo& other)
        : pkDataType{other.pkDataType->copy()}, index{other.index},
          batchInsertSharedState{other.batchInsertSharedState}, keyVectorPos{other.keyVectorPos},
          resultVectorPos{other.resultVectorPos} {}

    inline std::unique_ptr<IndexLookupInfo> copy() {
        return std::make_unique<IndexLookupInfo>(*this);
    }
};

class IndexLookup : public PhysicalOperator {
public:
    IndexLookup(std::vector<std::unique_ptr<IndexLookupInfo>> infos,
        std::unique_ptr<PhysicalOperator> child, uint32_t id, const std::string& paramsString)
        : PhysicalOperator{PhysicalOperatorType::INDEX_LOOKUP, std::move(child), id, paramsString},
          infos{std::move(infos)} {}

    void setBatchInsertSharedState(std::shared_ptr<BatchInsertSharedState> sharedState);

    bool getNextTuplesInternal(ExecutionContext* context) final;

    std::unique_ptr<PhysicalOperator> clone() final;

private:
    void lookup(transaction::Transaction* transaction, const IndexLookupInfo& info);
    void checkNullKeys(common::ValueVector* keyVector);
    void fillOffsetArraysFromVector(transaction::Transaction* transaction,
        const IndexLookupInfo& info, common::ValueVector* keyVector,
        common::ValueVector* resultVector);

private:
    std::vector<std::unique_ptr<IndexLookupInfo>> infos;
};

} // namespace processor
} // namespace kuzu
