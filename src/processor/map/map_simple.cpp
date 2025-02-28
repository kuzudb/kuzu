#include "common/exception/runtime.h"
#include "common/file_system/virtual_file_system.h"
#include "main/client_context.h"
#include "planner/operator/simple/logical_attach_database.h"
#include "planner/operator/simple/logical_detach_database.h"
#include "planner/operator/simple/logical_export_db.h"
#include "planner/operator/simple/logical_extension.h"
#include "planner/operator/simple/logical_import_db.h"
#include "planner/operator/simple/logical_use_database.h"
#include "processor/operator/simple/attach_database.h"
#include "processor/operator/simple/detach_database.h"
#include "processor/operator/simple/export_db.h"
#include "processor/operator/simple/import_db.h"
#include "processor/operator/simple/install_extension.h"
#include "processor/operator/simple/load_extension.h"
#include "processor/operator/simple/use_database.h"
#include "processor/plan_mapper.h"

namespace kuzu {
namespace processor {

using namespace kuzu::planner;
using namespace kuzu::common;
using namespace kuzu::storage;

static DataPos getOutputPos(const LogicalSimple* logicalSimple) {
    auto outSchema = logicalSimple->getSchema();
    auto outputExpression = logicalSimple->getOutputExpression();
    return DataPos(outSchema->getExpressionPos(*outputExpression));
}

std::unique_ptr<PhysicalOperator> PlanMapper::mapUseDatabase(
    const LogicalOperator* logicalOperator) {
    auto useDatabase = logicalOperator->constPtrCast<LogicalUseDatabase>();
    auto printInfo = std::make_unique<UseDatabasePrintInfo>(useDatabase->getDBName());
    return std::make_unique<UseDatabase>(useDatabase->getDBName(), getOutputPos(useDatabase),
        getOperatorID(), std::move(printInfo));
}

std::unique_ptr<PhysicalOperator> PlanMapper::mapAttachDatabase(
    const LogicalOperator* logicalOperator) {
    auto attachDatabase = logicalOperator->constPtrCast<LogicalAttachDatabase>();
    auto printInfo = std::make_unique<AttachDatabasePrintInfo>(
        attachDatabase->getAttachInfo().dbAlias, attachDatabase->getAttachInfo().dbPath);
    return std::make_unique<AttachDatabase>(attachDatabase->getAttachInfo(),
        getOutputPos(attachDatabase), getOperatorID(), std::move(printInfo));
}

std::unique_ptr<PhysicalOperator> PlanMapper::mapDetachDatabase(
    const LogicalOperator* logicalOperator) {
    auto detachDatabase = logicalOperator->constPtrCast<LogicalDetachDatabase>();
    auto printInfo = std::make_unique<OPPrintInfo>();
    return std::make_unique<DetachDatabase>(detachDatabase->getDBName(),
        getOutputPos(detachDatabase), getOperatorID(), std::move(printInfo));
}

std::unique_ptr<PhysicalOperator> PlanMapper::mapExportDatabase(
    const LogicalOperator* logicalOperator) {
    auto exportDatabase = logicalOperator->constPtrCast<LogicalExportDatabase>();
    auto fs = clientContext->getVFSUnsafe();
    auto boundFileInfo = exportDatabase->getBoundFileInfo();
    KU_ASSERT(boundFileInfo->filePaths.size() == 1);
    auto filePath = boundFileInfo->filePaths[0];
    if (fs->fileOrPathExists(filePath, clientContext)) {
        throw RuntimeException(stringFormat("Directory {} already exists.", filePath));
    }
    std::vector<std::unique_ptr<PhysicalOperator>> children;
    for (auto childCopyTo : exportDatabase->getChildren()) {
        auto childPhysicalOperator = mapOperator(childCopyTo.get());
        children.push_back(std::move(childPhysicalOperator));
    }
    auto printInfo = std::make_unique<ExportDBPrintInfo>(filePath, boundFileInfo->options);
    auto exportDB = std::make_unique<ExportDB>(exportDatabase->getBoundFileInfo()->copy(),
        getOutputPos(exportDatabase), getOperatorID(), std::move(printInfo));
    auto outputExpr = {exportDatabase->getOutputExpression()};
    auto resultCollector = createResultCollector(AccumulateType::REGULAR, outputExpr,
        exportDatabase->getSchema(), std::move(exportDB));
    auto resultFT = resultCollector->getResultFactorizedTable();
    children.push_back(std::move(resultCollector));
    return createFTableScan(outputExpr, {0} /* colIdxes */, exportDatabase->getSchema(), resultFT,
        1 /* maxMorselSize */, std::move(children));
}

std::unique_ptr<PhysicalOperator> PlanMapper::mapImportDatabase(
    const LogicalOperator* logicalOperator) {
    auto importDatabase = logicalOperator->constPtrCast<LogicalImportDatabase>();
    auto printInfo = std::make_unique<OPPrintInfo>();
    return std::make_unique<ImportDB>(importDatabase->getQuery(), importDatabase->getIndexQuery(),
        getOutputPos(importDatabase), getOperatorID(), std::move(printInfo));
}

std::unique_ptr<PhysicalOperator> PlanMapper::mapExtension(const LogicalOperator* logicalOperator) {
    auto logicalExtension = logicalOperator->constPtrCast<LogicalExtension>();
    auto outputPos = getOutputPos(logicalExtension);
    std::unique_ptr<OPPrintInfo> printInfo;
    auto& auxInfo = logicalExtension->getAuxInfo();
    auto path = auxInfo.path;
    switch (auxInfo.action) {
    case ExtensionAction::INSTALL: {
        extension::InstallExtensionInfo info{path,
            auxInfo.contCast<InstallExtensionAuxInfo>().extensionRepo};
        printInfo = std::make_unique<InstallExtensionPrintInfo>(path);
        return std::make_unique<InstallExtension>(std::move(info), outputPos, getOperatorID(),
            std::move(printInfo));
    }
    case ExtensionAction::LOAD: {
        printInfo = std::make_unique<LoadExtensionPrintInfo>(path);
        return std::make_unique<LoadExtension>(path, outputPos, getOperatorID(),
            std::move(printInfo));
    }
    default:
        KU_UNREACHABLE;
    }
}

} // namespace processor
} // namespace kuzu
