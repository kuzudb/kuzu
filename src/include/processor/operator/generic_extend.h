#pragma once

#include "processor/operator/physical_operator.h"
#include "storage/storage_structure/column.h"
#include "storage/storage_structure/lists/lists.h"

namespace kuzu {
namespace processor {

struct ColumnAndListCollection {
    vector<Column*> columns;
    vector<Lists*> lists;
    vector<shared_ptr<ListHandle>> listHandles;
};

class GenericExtend : public PhysicalOperator {
public:
    GenericExtend(const DataPos& inVectorPos, const DataPos& outNodeVectorPos,
        ColumnAndListCollection adjColumnAndListCollection, vector<DataPos> outPropertyVectorsPos,
        vector<ColumnAndListCollection> propertyColumnAndListCollections,
        unique_ptr<PhysicalOperator> child, uint32_t id, const string& paramsString)
        : PhysicalOperator{std::move(child), id, paramsString}, inVectorPos{inVectorPos},
          outNodeVectorPos{outNodeVectorPos}, adjColumnAndListCollection{std::move(
                                                  adjColumnAndListCollection)},
          outPropertyVectorsPos{std::move(outPropertyVectorsPos)},
          propertyColumnAndListCollections{std::move(propertyColumnAndListCollections)} {}
    ~GenericExtend() override = default;

    inline PhysicalOperatorType getOperatorType() override {
        return PhysicalOperatorType::GENERIC_EXTEND;
    }

    void initLocalStateInternal(ResultSet* resultSet, ExecutionContext* context) override;

    bool getNextTuplesInternal() override;

    unique_ptr<PhysicalOperator> clone() override {
        return make_unique<GenericExtend>(inVectorPos, outNodeVectorPos, adjColumnAndListCollection,
            outPropertyVectorsPos, propertyColumnAndListCollections, children[0]->clone(), id,
            paramsString);
    }

private:
    bool scan();

    inline bool hasColumnToScan() const {
        return nextColumnIdx < adjColumnAndListCollection.columns.size();
    }
    inline bool hasListToScan() const {
        return nextListIdx < adjColumnAndListCollection.lists.size();
    }
    bool scanColumns();
    bool scanLists();
    bool scanColumn(uint32_t idx);
    bool scanList(uint32_t idx);

private:
    DataPos inVectorPos;
    DataPos outNodeVectorPos;
    ColumnAndListCollection adjColumnAndListCollection;
    vector<DataPos> outPropertyVectorsPos;
    vector<ColumnAndListCollection> propertyColumnAndListCollections;

    shared_ptr<ValueVector> inVector;
    shared_ptr<ValueVector> outNodeVector;
    vector<shared_ptr<ValueVector>> outPropertyVectors;
    unique_ptr<ListSyncState> listSyncState;
    uint32_t nextColumnIdx;
    uint32_t nextListIdx;
    // A list may be scanned for multi getNext() call e.g. large list. So we track current list.
    AdjLists* currentAdjList;
    ListHandle* currentAdjListHandle;
    vector<Lists*> currentPropertyLists;
    vector<ListHandle*> currentPropertyListHandles;
    node_offset_t currentNodeOffset;
};

} // namespace processor
} // namespace kuzu
