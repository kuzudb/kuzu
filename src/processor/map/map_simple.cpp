#include "common/exception/runtime.h"
#include "common/file_system/virtual_file_system.h"
#include "planner/operator/simple/logical_attach_database.h"
#include "planner/operator/simple/logical_detach_database.h"
#include "planner/operator/simple/logical_export_db.h"
#include "planner/operator/simple/logical_import_db.h"
#include "planner/operator/simple/logical_use_database.h"
#include "processor/operator/simple/attach_database.h"
#include "processor/operator/simple/detach_database.h"
#include "processor/operator/simple/export_db.h"
#include "processor/operator/simple/import_db.h"
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
    return std::make_unique<UseDatabase>(useDatabase->getDBName(), getOutputPos(useDatabase),
        getOperatorID(), useDatabase->getExpressionsForPrinting());
}

std::unique_ptr<PhysicalOperator> PlanMapper::mapAttachDatabase(
    planner::LogicalOperator* logicalOperator) {
    auto attachDatabase = logicalOperator->constPtrCast<LogicalAttachDatabase>();
    return std::make_unique<AttachDatabase>(attachDatabase->getAttachInfo(),
        getOutputPos(attachDatabase), getOperatorID(), attachDatabase->getExpressionsForPrinting());
}

std::unique_ptr<PhysicalOperator> PlanMapper::mapDetachDatabase(
    planner::LogicalOperator* logicalOperator) {
    auto detachDatabase = logicalOperator->constPtrCast<LogicalDetachDatabase>();
    return std::make_unique<DetachDatabase>(detachDatabase->getDBName(),
        getOutputPos(detachDatabase), getOperatorID(), detachDatabase->getExpressionsForPrinting());
}

std::unique_ptr<PhysicalOperator> PlanMapper::mapExportDatabase(
    planner::LogicalOperator* logicalOperator) {
    auto exportDatabase =
        ku_dynamic_cast<LogicalOperator*, LogicalExportDatabase*>(logicalOperator);
    std::vector<std::unique_ptr<PhysicalOperator>> children;
    for (auto childCopyTo : exportDatabase->getChildren()) {
        auto childPhysicalOperator = mapOperator(childCopyTo.get());
        children.push_back(std::move(childPhysicalOperator));
    }
    // Ideally we should create directory inside operator but ExportDatabase is executed before
    // CopyTo which requires the directory to exist. So we create directory in mapper inside.
    auto fs = clientContext->getVFSUnsafe();
    auto boundFileInfo = exportDatabase->getBoundFileInfo();
    KU_ASSERT(boundFileInfo->filePaths.size() == 1);
    auto filePath = boundFileInfo->filePaths[0];
    if (!fs->fileOrPathExists(filePath)) {
        fs->createDir(filePath);
    } else {
        throw RuntimeException(stringFormat("Directory {} already exists.", filePath));
    }
    return std::make_unique<ExportDB>(exportDatabase->getBoundFileInfo()->copy(),
        getOutputPos(exportDatabase), getOperatorID(), exportDatabase->getExpressionsForPrinting(),
        std::move(children));
}

std::unique_ptr<PhysicalOperator> PlanMapper::mapImportDatabase(
    planner::LogicalOperator* logicalOperator) {
    auto importDatabase =
        ku_dynamic_cast<LogicalOperator*, LogicalImportDatabase*>(logicalOperator);
    return std::make_unique<ImportDB>(importDatabase->getQuery(), getOutputPos(importDatabase),
        getOperatorID(), importDatabase->getExpressionsForPrinting());
}

} // namespace processor
} // namespace kuzu
