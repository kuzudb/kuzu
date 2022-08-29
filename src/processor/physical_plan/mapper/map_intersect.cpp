#include "src/planner/logical_plan/logical_operator/include/logical_intersect.h"
#include "src/processor/include/physical_plan/mapper/plan_mapper.h"
#include "src/processor/include/physical_plan/operator/hash_join/hash_join_build.h"
#include "src/processor/include/physical_plan/operator/intersect.h"
#include "src/processor/include/physical_plan/operator/intersect_build.h"
#include "src/processor/include/physical_plan/operator/semi_masker.h"

using namespace graphflow::planner;
using namespace graphflow::processor;

namespace graphflow {
namespace processor {

static void mapASPIntersect(Intersect* intersect) {
    auto tableScan = intersect->getChild(0);
    while (tableScan->getOperatorType() != FACTORIZED_TABLE_SCAN) {
        assert(tableScan->getNumChildren() != 0);
        tableScan = tableScan->getChild(0);
    }
    assert(tableScan->getChild(0)->getOperatorType() == RESULT_COLLECTOR);
    auto resultCollector = tableScan->moveUnaryChild();
    assert(tableScan->getNumChildren() == 0);
    intersect->addChild(move(resultCollector));
}

// Input logical plan
//            Intersect
// probe-plan scan-edge-1 scan-edge-2
//
// Output physical plan
//            Intersect
// probe-2    build-1    build-2
// probe-1    sorter     sorter
// probe-plan scan-edge-1 scan-edge-2

unique_ptr<PhysicalOperator> PlanMapper::mapLogicalIntersectToPhysical(
    LogicalOperator* logicalOperator, MapperContext& mapperContext) {
    auto logicalIntersect = (LogicalIntersect*)logicalOperator;
    auto intersectNodeID = logicalIntersect->getIntersectNode()->getIDProperty();
    vector<unique_ptr<PhysicalOperator>> children;
    //    vector<shared_ptr<HashJoinSharedState>> sharedStates;
    vector<shared_ptr<IntersectHashTable>> sharedHTs;
    children.push_back(mapLogicalOperatorToPhysical(logicalIntersect->getChild(0), mapperContext));
    auto probeSidePrevOperator = children[0].get();
    vector<DataPos> probeSideKeyVectorsPos;
    auto numBuild = logicalIntersect->getNumChildren() - 1;
    for (auto i = 1u; i < logicalIntersect->getNumChildren(); ++i) {
        auto logicalBuildChild = logicalIntersect->getChild(i);
        auto logicalBuildInfo = logicalIntersect->getBuildInfo(i - 1);
        auto key = logicalBuildInfo->key;
        auto buildSideMapperContext =
            MapperContext(make_unique<ResultSetDescriptor>(*logicalBuildInfo->schema));
        // S(a)E(e1)
        // map build child plan
        auto buildSidePrevOperator =
            mapLogicalOperatorToPhysical(logicalBuildChild, buildSideMapperContext);
        if (logicalIntersect->getIntersectType() ==
            IntersectType::ASP_MW_JOIN) { // config semi masker
            auto scanNodeIDs = collectScanNodeID(buildSidePrevOperator.get());
            assert(scanNodeIDs.size() == 1);
            auto scanNodeID = (ScanNodeID*)*scanNodeIDs.begin();

            auto op = probeSidePrevOperator;
            while (op->getOperatorType() != SEMI_MASKER) {
                op = op->getChild(0);
            }
            auto buildIdx = i - 1;
            for (auto j = 0; j < numBuild - buildIdx - 1; ++j) {
                op = op->getChild(0);
            }
            assert(op->getOperatorType() == SEMI_MASKER);
            auto semiMasker = (SemiMasker*)op;
            assert(semiMasker->nodeID == scanNodeID->nodeID);
            semiMasker->setSharedState(scanNodeID->getSharedState());
        }

        auto keyDataPos = buildSideMapperContext.getDataPos(key->getIDProperty());
        assert(keyDataPos.dataChunkPos == 0 && keyDataPos.valueVectorPos == 0);
        // add build on top. Note: payload refers to non-key in map_hash_join.cpp
        // S(a)E(e1)SORT(c)BUILD(a->c)
        vector<DataPos> payloadDataPoses;
        vector<bool> isPayloadsFlat;
        vector<DataType> payloadDataTypes;
        for (auto& expression : logicalBuildInfo->expressionsToMaterialize) {
            auto expressionName = expression->getUniqueName();
            if (expressionName == key->getIDProperty()) {
                continue;
            }
            auto payloadDataPos = buildSideMapperContext.getDataPos(expressionName);
            assert(payloadDataPos.dataChunkPos == 1 && payloadDataPos.valueVectorPos == 0);
            payloadDataPoses.push_back(payloadDataPos);
            assert(expression->dataType.typeID == NODE_ID);
            payloadDataTypes.push_back(expression->dataType);
            auto isPayloadFlat = logicalBuildInfo->schema->getGroup(expressionName)->getIsFlat();
            assert(!isPayloadFlat);
            isPayloadsFlat.push_back(isPayloadFlat);
        }
        assert(payloadDataPoses.size() == 1);
        auto buildDataInfo = BuildDataInfo(keyDataPos, payloadDataPoses, isPayloadsFlat);
        probeSideKeyVectorsPos.push_back(mapperContext.getDataPos(key->getIDProperty()));
        //        auto sharedState = make_shared<HashJoinSharedState>(payloadDataTypes);
        //        sharedStates.push_back(sharedState);
        auto maxNumNodesForKey = storageManager.getNodesStore().getNodesMetadata().getMaxNodeOffset(
            nullptr, key->getLabel());
        auto sharedHT = make_shared<IntersectHashTable>(*memoryManager, maxNumNodesForKey);
        sharedHTs.push_back(sharedHT);
        auto intersectBuild = make_unique<IntersectBuild>(
            sharedHT, move(buildSidePrevOperator), getOperatorID(), key->getRawName());
        //        auto hashJoinBuild = make_unique<HashJoinBuild>(sharedState, buildDataInfo,
        //            move(buildSidePrevOperator), getOperatorID(), key->getRawName());
        children.push_back(move(intersectBuild));
    }
    auto outputVectorPos =
        mapperContext.getDataPos(logicalIntersect->getIntersectNode()->getIDProperty());
    auto intersect = make_unique<Intersect>(logicalIntersect->getIntersectNode()->getLabel(),
        outputVectorPos, probeSideKeyVectorsPos, move(sharedHTs), move(children), getOperatorID(),
        logicalIntersect->getIntersectNode()->getUniqueName());
    if (logicalIntersect->getIntersectType() == planner::IntersectType::ASP_MW_JOIN) {
        mapASPIntersect(intersect.get());
    }
    return intersect;
}

} // namespace processor
} // namespace graphflow