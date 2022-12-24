#pragma once

#include "processor/operator/base_extend.h"
#include "storage/storage_structure/lists/lists.h"

namespace kuzu {
namespace processor {

class ScanRelTableLists : public BaseExtendAndScanRelProperties {
public:
    ScanRelTableLists(const DataPos& inNodeIDVectorPos, const DataPos& outNodeIDVectorPos,
        vector<DataPos> outPropertyVectorsPos, Lists* adjList, vector<Lists*> propertyLists,
        unique_ptr<PhysicalOperator> child, uint32_t id, const string& paramsString)
        : BaseExtendAndScanRelProperties{PhysicalOperatorType::LIST_EXTEND, inNodeIDVectorPos,
              outNodeIDVectorPos, std::move(outPropertyVectorsPos), std::move(child), id,
              paramsString},
          adjList{adjList}, propertyLists{std::move(propertyLists)} {}
    ~ScanRelTableLists() override = default;

    void initLocalStateInternal(ResultSet* resultSet, ExecutionContext* context) override;

    bool getNextTuplesInternal() override;

    inline unique_ptr<PhysicalOperator> clone() override {
        return make_unique<ScanRelTableLists>(inNodeIDVectorPos, outNodeIDVectorPos,
            outPropertyVectorsPos, adjList, propertyLists, children[0]->clone(), id, paramsString);
    }

private:
    void scanPropertyLists();

private:
    // lists
    Lists* adjList;
    vector<Lists*> propertyLists;
    // list handles
    shared_ptr<ListHandle> adjListHandle;
    vector<shared_ptr<ListHandle>> propertyListHandles;
    // sync state between adj and property lists
    unique_ptr<ListSyncState> syncState;
};

} // namespace processor
} // namespace kuzu
