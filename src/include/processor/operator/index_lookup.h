#pragma once

#include "processor/operator/physical_operator.h"

namespace kuzu {
namespace storage {
class PrimaryKeyIndex;
}
namespace processor {

struct IndexLookupInfo {
    common::table_id_t tableID;
    std::unique_ptr<common::LogicalType> pkDataType;
    storage::PrimaryKeyIndex* pkIndex; // NULL if the PK data type is SERIAL.
    DataPos keyVectorPos;
    DataPos resultVectorPos;

    IndexLookupInfo(common::table_id_t tableID, std::unique_ptr<common::LogicalType> pkDataType,
        storage::PrimaryKeyIndex* pkIndex, const DataPos& keyVectorPos,
        const DataPos& resultVectorPos)
        : tableID{tableID}, pkDataType{std::move(pkDataType)}, pkIndex{pkIndex},
          keyVectorPos{keyVectorPos}, resultVectorPos{resultVectorPos} {}

    inline std::unique_ptr<IndexLookupInfo> copy() {
        return std::make_unique<IndexLookupInfo>(
            tableID, pkDataType->copy(), pkIndex, keyVectorPos, resultVectorPos);
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
    void indexLookup(transaction::Transaction* transaction, const IndexLookupInfo& info);
    void checkNullKeys(common::ValueVector* keyVector);
    void fillOffsetArraysFromVector(transaction::Transaction* transaction,
        const IndexLookupInfo& info, common::ValueVector* keyVector,
        common::ValueVector* resultVector);

private:
    std::vector<std::unique_ptr<IndexLookupInfo>> infos;
};

} // namespace processor
} // namespace kuzu
