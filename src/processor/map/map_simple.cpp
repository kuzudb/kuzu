#include "common/exception/runtime.h"
#include "common/file_system/virtual_file_system.h"
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
    planner::LogicalOperator* logicalOperator) {
    auto useDatabase = logicalOperator->constPtrCast<LogicalUseDatabase>();
    auto printInfo = std::make_unique<OPPrintInfo>(useDatabase->getExpressionsForPrinting());
    return std::make_unique<UseDatabase>(useDatabase->getDBName(), getOutputPos(useDatabase),
        getOperatorID(), std::move(printInfo));
}

std::unique_ptr<PhysicalOperator> PlanMapper::mapAttachDatabase(
    planner::LogicalOperator* logicalOperator) {
    auto attachDatabase = logicalOperator->constPtrCast<LogicalAttachDatabase>();
    auto printInfo = std::make_unique<AttachDatabasePrintInfo>(
        attachDatabase->getAttachInfo().dbAlias, attachDatabase->getAttachInfo().dbPath);
    return std::make_unique<AttachDatabase>(attachDatabase->getAttachInfo(),
        getOutputPos(attachDatabase), getOperatorID(), std::move(printInfo));
}

std::unique_ptr<PhysicalOperator> PlanMapper::mapDetachDatabase(
    planner::LogicalOperator* logicalOperator) {
    auto detachDatabase = logicalOperator->constPtrCast<LogicalDetachDatabase>();
    auto printInfo = std::make_unique<OPPrintInfo>(detachDatabase->getExpressionsForPrinting());
    return std::make_unique<DetachDatabase>(detachDatabase->getDBName(),
        getOutputPos(detachDatabase), getOperatorID(), std::move(printInfo));
}

std::unique_ptr<PhysicalOperator> PlanMapper::mapExportDatabase(
    planner::LogicalOperator* logicalOperator) {
    auto exportDatabase = logicalOperator->constPtrCast<LogicalExportDatabase>();
    // Ideally we should create directory inside operator but ExportDatabase is executed before
    // CopyTo which requires the directory to exist. So we create directory in mapper inside.
    auto fs = clientContext->getVFSUnsafe();
    auto boundFileInfo = exportDatabase->getBoundFileInfo();
    KU_ASSERT(boundFileInfo->filePaths.size() == 1);
    auto filePath = boundFileInfo->filePaths[0];
    if (!fs->fileOrPathExists(filePath, clientContext)) {
        fs->createDir(filePath);
    } else {
        throw RuntimeException(stringFormat("Directory {} already exists.", filePath));
    }
    std::vector<std::unique_ptr<PhysicalOperator>> children;
    for (auto childCopyTo : exportDatabase->getChildren()) {
        auto childPhysicalOperator = mapOperator(childCopyTo.get());
        children.push_back(std::move(childPhysicalOperator));
    }
    auto printInfo = std::make_unique<OPPrintInfo>(exportDatabase->getExpressionsForPrinting());
    return std::make_unique<ExportDB>(exportDatabase->getBoundFileInfo()->copy(),
        getOutputPos(exportDatabase), getOperatorID(), std::move(children), std::move(printInfo));
}

std::unique_ptr<PhysicalOperator> PlanMapper::mapImportDatabase(
    planner::LogicalOperator* logicalOperator) {
    auto importDatabase = logicalOperator->constPtrCast<LogicalImportDatabase>();
    auto printInfo = std::make_unique<OPPrintInfo>(importDatabase->getExpressionsForPrinting());
    return std::make_unique<ImportDB>(importDatabase->getQuery(), getOutputPos(importDatabase),
        getOperatorID(), std::move(printInfo));
}

std::unique_ptr<PhysicalOperator> PlanMapper::mapExtension(
    planner::LogicalOperator* logicalOperator) {
    auto logicalExtension = logicalOperator->constPtrCast<LogicalExtension>();
    auto outputPos = getOutputPos(logicalExtension);
    auto printInfo = std::make_unique<OPPrintInfo>(logicalExtension->getExpressionsForPrinting());
    switch (logicalExtension->getAction()) {
    case ExtensionAction::INSTALL:
        return std::make_unique<InstallExtension>(logicalExtension->getPath(), outputPos,
            getOperatorID(), std::move(printInfo));
    case ExtensionAction::LOAD:
        return std::make_unique<LoadExtension>(logicalExtension->getPath(), outputPos,
            getOperatorID(), std::move(printInfo));
    default:
        KU_UNREACHABLE;
    }
}

} // namespace processor
} // namespace kuzu
