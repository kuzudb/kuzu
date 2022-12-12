#pragma once

#include "processor/operator/base_extend.h"
#include "storage/storage_structure/column.h"
#include "storage/storage_structure/lists/lists.h"

namespace kuzu {
namespace processor {

// Each output vector is scanned from a collection of Columns and Lists
struct ColumnAndListCollection {
    vector<Column*> columns;
    vector<Lists*> lists;
    vector<shared_ptr<ListHandle>> listHandles;

    ColumnAndListCollection(vector<Column*> columns, vector<Lists*> lists)
        : columns{std::move(columns)}, lists{std::move(lists)} {}

    void populateListHandles(ListSyncState& syncState);
};

class AdjAndPropertyCollection {
public:
    AdjAndPropertyCollection(unique_ptr<ColumnAndListCollection> adjCollection,
        vector<unique_ptr<ColumnAndListCollection>> propertyCollections)
        : adjCollection{std::move(adjCollection)}, propertyCollections{
                                                       std::move(propertyCollections)} {}

    void populateListHandles();

    void resetState(node_offset_t nodeOffset);

    bool scan(const shared_ptr<ValueVector>& inVector, const shared_ptr<ValueVector>& outNodeVector,
        const vector<shared_ptr<ValueVector>>& outPropertyVectors, Transaction* transaction);

    unique_ptr<AdjAndPropertyCollection> clone() const;

private:
    bool scanColumns(const shared_ptr<ValueVector>& inVector,
        const shared_ptr<ValueVector>& outNodeVector,
        const vector<shared_ptr<ValueVector>>& outPropertyVectors, Transaction* transaction);
    bool scanLists(const shared_ptr<ValueVector>& inVector,
        const shared_ptr<ValueVector>& outNodeVector,
        const vector<shared_ptr<ValueVector>>& outPropertyVectors, Transaction* transaction);

    inline bool hasColumnToScan() const { return nextColumnIdx < adjCollection->columns.size(); }
    inline bool hasListToScan() const { return nextListIdx < adjCollection->lists.size(); }

    bool scanColumn(uint32_t idx, const shared_ptr<ValueVector>& inVector,
        const shared_ptr<ValueVector>& outNodeVector,
        const vector<shared_ptr<ValueVector>>& outPropertyVectors, Transaction* transaction);
    bool scanList(uint32_t idx, const shared_ptr<ValueVector>& inVector,
        const shared_ptr<ValueVector>& outNodeVector,
        const vector<shared_ptr<ValueVector>>& outPropertyVectors, Transaction* transaction);
    void scanPropertyList(uint32_t idx, const vector<shared_ptr<ValueVector>>& outPropertyVectors,
        Transaction* transaction);

private:
    unique_ptr<ColumnAndListCollection> adjCollection;
    vector<unique_ptr<ColumnAndListCollection>> propertyCollections;

    // Next column idx to scan.
    uint32_t nextColumnIdx = UINT32_MAX;
    // Next list idx to scan.
    uint32_t nextListIdx = UINT32_MAX;
    // Current node offset to extend from.
    node_offset_t currentNodeOffset = INVALID_NODE_OFFSET;
    // Current list idx to scan. Note that a list may be scanned multiple times.
    uint32_t currentListIdx = UINT32_MAX;
    // Sync between adjList and propertyLists
    unique_ptr<ListSyncState> listSyncState = nullptr;
};

class GenericExtendAndScanRelProperties : public BaseExtendAndScanRelProperties {
public:
    GenericExtendAndScanRelProperties(const DataPos& inNodeIDVectorPos,
        const DataPos& outNodeIDVectorPos, vector<DataPos> outPropertyVectorsPos,
        unordered_map<table_id_t, unique_ptr<AdjAndPropertyCollection>>
            adjAndPropertyCollectionPerNodeTable,
        unique_ptr<PhysicalOperator> child, uint32_t id, const string& paramsString)
        : BaseExtendAndScanRelProperties{PhysicalOperatorType::GENERIC_EXTEND, inNodeIDVectorPos,
              outNodeIDVectorPos, std::move(outPropertyVectorsPos), std::move(child), id,
              paramsString},
          adjAndPropertyCollectionPerNodeTable{std::move(adjAndPropertyCollectionPerNodeTable)} {}
    ~GenericExtendAndScanRelProperties() override = default;

    void initLocalStateInternal(ResultSet* resultSet, ExecutionContext* context) override;

    bool getNextTuplesInternal() override;

    unique_ptr<PhysicalOperator> clone() override {
        unordered_map<table_id_t, unique_ptr<AdjAndPropertyCollection>> clonedCollections;
        for (auto& [tableID, adjAndPropertyCollection] : adjAndPropertyCollectionPerNodeTable) {
            clonedCollections.insert({tableID, adjAndPropertyCollection->clone()});
        }
        return make_unique<GenericExtendAndScanRelProperties>(inNodeIDVectorPos, outNodeIDVectorPos,
            outPropertyVectorsPos, std::move(clonedCollections), children[0]->clone(), id,
            paramsString);
    }

private:
    bool scanCurrentAdjAndPropertyCollection();
    void initCurrentAdjAndPropertyCollection(const nodeID_t& nodeID);

private:
    unordered_map<table_id_t, unique_ptr<AdjAndPropertyCollection>>
        adjAndPropertyCollectionPerNodeTable;
    AdjAndPropertyCollection* currentAdjAndPropertyCollection = nullptr;
};

} // namespace processor
} // namespace kuzu
