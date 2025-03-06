#include "binder/expression/node_expression.h"
#include "graph/on_disk_graph.h"
#include "planner/operator/logical_gds_call.h"
#include "planner/operator/sip/logical_semi_masker.h"
#include "processor/operator/gds_call.h"
#include "processor/plan_mapper.h"
#include "storage/storage_manager.h"

using namespace kuzu::binder;
using namespace kuzu::common;
using namespace kuzu::graph;
using namespace kuzu::function;
using namespace kuzu::planner;
using namespace kuzu::storage;

namespace kuzu {
namespace processor {

static std::unique_ptr<RoaringBitmapSemiMask> getTableMask(table_id_t tableID,
    StorageManager* storageManager, transaction::Transaction* transaction) {
    auto table = storageManager->getTable(tableID)->ptrCast<NodeTable>();
    return RoaringBitmapSemiMaskUtil::createMask(table->getNumTotalRows(transaction));
}

std::unique_ptr<PhysicalOperator> PlanMapper::mapGDSCall(const LogicalOperator* logicalOperator) {
    auto& call = logicalOperator->constCast<LogicalGDSCall>();
    auto& logicalInfo = call.getInfo();
    auto outSchema = call.getSchema();
    auto tableSchema = std::make_unique<FactorizedTableSchema>();
    auto columns = logicalInfo.outExprs;
    for (auto& expr : columns) {
        auto dataPos = getDataPos(*expr, *outSchema);
        auto columnSchema = ColumnSchema(false /* isUnFlat */, dataPos.dataChunkPos,
            LogicalTypeUtils::getRowLayoutSize(expr->getDataType()));
        tableSchema->appendColumn(std::move(columnSchema));
    }
    auto table =
        std::make_shared<FactorizedTable>(clientContext->getMemoryManager(), tableSchema->copy());
    auto graph =
        std::make_unique<OnDiskGraph>(clientContext, logicalInfo.getBindData()->graphEntry.copy());
    auto storageManager = clientContext->getStorageManager();
    auto sharedState =
        std::make_shared<GDSCallSharedState>(table, std::move(graph), call.getLimitNum());
    if (call.hasNbrTableIDSet()) {
        sharedState->setNbrTableIDSet(call.getNbrTableIDSet());
    }
    auto bindData = call.getInfo().getBindData();
    auto transaction = clientContext->getTransaction();
    if (bindData->hasNodeInput()) {
        auto& node = bindData->getNodeInput()->constCast<NodeExpression>();
        auto maskMap = std::make_unique<NodeOffsetMaskMap>();
        for (auto tableID : node.getTableIDs()) {
            maskMap->addMask(tableID, getTableMask(tableID, storageManager, transaction));
        }
        sharedState->setInputNodeMask(std::move(maskMap));
    }
    if (bindData->hasNodeOutput()) {
        auto& node = bindData->getNodeOutput()->constCast<NodeExpression>();
        auto maskMap = std::make_unique<NodeOffsetMaskMap>();
        for (auto tableID : node.getTableIDs()) {
            maskMap->addMask(tableID, getTableMask(tableID, storageManager, transaction));
        }
        sharedState->setOutputNodeMask(std::move(maskMap));
    }
    auto printInfo = std::make_unique<GDSCallPrintInfo>(call.getInfo().func.name);
    auto gdsAlgorithm = call.getInfo().getGDS()->copy();
    auto descriptor = std::make_unique<ResultSetDescriptor>();
    auto gdsCall = std::make_unique<GDSCall>(std::move(descriptor), std::move(gdsAlgorithm),
        sharedState, getOperatorID(), std::move(printInfo));
    // Map node predicate pipeline
    if (call.hasNodePredicate()) {
        auto& maskInfo = call.getMaskInfo();
        logicalOpToPhysicalOpMap.insert({logicalOperator, gdsCall.get()});
        if (maskInfo.isPathNodeMask) {
            sharedState->setPathNodeMask(std::make_unique<NodeOffsetMaskMap>());
            auto maskMap = sharedState->getPathNodeMaskMap();
            KU_ASSERT(maskInfo.maskRoots.size() == 1);
            auto logicalRoot = maskInfo.maskRoots[0];
            auto logicalSemiMasker = logicalRoot->ptrCast<LogicalSemiMasker>();
            logicalSemiMasker->addTarget(logicalOperator);
            for (auto tableID : logicalSemiMasker->getNodeTableIDs()) {
                maskMap->addMask(tableID, getTableMask(tableID, storageManager, transaction));
            }
            auto root = mapOperator(logicalRoot.get());
            auto dummySink = std::make_unique<DummySink>(
                std::make_unique<ResultSetDescriptor>(logicalRoot->getSchema()), std::move(root),
                getOperatorID(), std::make_unique<OPPrintInfo>());
            gdsCall->addChild(std::move(dummySink));
        } else {
            sharedState->setGraphNodeMask(std::make_unique<NodeOffsetMaskMap>());

            auto maskMap = sharedState->getGraphNodeMaskMap();
            for (auto logicalRoot : maskInfo.maskRoots) {
                auto logicalSemiMasker = logicalRoot->ptrCast<LogicalSemiMasker>();
                logicalSemiMasker->addTarget(logicalOperator);
                KU_ASSERT(logicalSemiMasker->getNodeTableIDs().size() == 1);
                for (auto tableID : logicalSemiMasker->getNodeTableIDs()) {
                    maskMap->addMask(tableID, getTableMask(tableID, storageManager, transaction));
                }
                auto root = mapOperator(logicalRoot.get());
                auto dummySink = std::make_unique<DummySink>(
                    std::make_unique<ResultSetDescriptor>(logicalRoot->getSchema()),
                    std::move(root), getOperatorID(), std::make_unique<OPPrintInfo>());
                gdsCall->addChild(std::move(dummySink));
            }
        }
        logicalOpToPhysicalOpMap.erase(logicalOperator);
    }
    logicalOpToPhysicalOpMap.insert({logicalOperator, gdsCall.get()});
    physical_op_vector_t children;
    children.push_back(std::move(gdsCall));
    return createFTableScanAligned(columns, outSchema, table, DEFAULT_VECTOR_CAPACITY,
        std::move(children));
}

} // namespace processor
} // namespace kuzu
