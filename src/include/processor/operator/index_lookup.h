#pragma once

#include "processor/operator/physical_operator.h"

namespace kuzu {
namespace storage {
class PrimaryKeyIndex;
} // namespace storage
namespace processor {

struct BatchInsertSharedState;
struct IndexLookupInfo {
    storage::PrimaryKeyIndex* index;
    // In copy rdf, we need to perform lookup before primary key is persist on disk. So we need to
    // use index builder.
    std::shared_ptr<BatchInsertSharedState> batchInsertSharedState;
    DataPos keyVectorPos;
    DataPos resultVectorPos;

    IndexLookupInfo(storage::PrimaryKeyIndex* index, const DataPos& keyVectorPos,
        const DataPos& resultVectorPos)
        : index{index}, batchInsertSharedState{nullptr}, keyVectorPos{keyVectorPos},
          resultVectorPos{resultVectorPos} {}
    IndexLookupInfo(const IndexLookupInfo& other)
        : index{other.index}, batchInsertSharedState{other.batchInsertSharedState},
          keyVectorPos{other.keyVectorPos}, resultVectorPos{other.resultVectorPos} {}

    inline std::unique_ptr<IndexLookupInfo> copy() {
        return std::make_unique<IndexLookupInfo>(*this);
    }
};

class IndexLookup : public PhysicalOperator {
    static constexpr PhysicalOperatorType type_ = PhysicalOperatorType::INDEX_LOOKUP;

public:
    IndexLookup(std::vector<std::unique_ptr<IndexLookupInfo>> infos,
        std::unique_ptr<PhysicalOperator> child, uint32_t id,
        std::unique_ptr<OPPrintInfo> printInfo)
        : PhysicalOperator{type_, std::move(child), id, std::move(printInfo)},
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
