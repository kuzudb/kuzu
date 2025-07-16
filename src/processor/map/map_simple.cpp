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
#include "processor/operator/simple/uninstall_extension.h"
#include "processor/operator/simple/use_database.h"
#include "processor/plan_mapper.h"
#include "processor/result/factorized_table_util.h"

namespace kuzu {
namespace processor {

using namespace kuzu::planner;
using namespace kuzu::common;
using namespace kuzu::storage;
using namespace kuzu::extension;

std::unique_ptr<PhysicalOperator> PlanMapper::mapUseDatabase(
    const LogicalOperator* logicalOperator) {
    auto useDatabase = logicalOperator->constPtrCast<LogicalUseDatabase>();
    auto printInfo = std::make_unique<UseDatabasePrintInfo>(useDatabase->getDBName());
    auto messageTable =
        FactorizedTableUtils::getSingleStringColumnFTable(clientContext->getMemoryManager());
    return std::make_unique<UseDatabase>(useDatabase->getDBName(), std::move(messageTable),
        getOperatorID(), std::move(printInfo));
}

std::unique_ptr<PhysicalOperator> PlanMapper::mapAttachDatabase(
    const LogicalOperator* logicalOperator) {
    auto attachDatabase = logicalOperator->constPtrCast<LogicalAttachDatabase>();
    auto info = attachDatabase->getAttachInfo();
    auto printInfo = std::make_unique<AttachDatabasePrintInfo>(info.dbAlias, info.dbPath);
    auto messageTable =
        FactorizedTableUtils::getSingleStringColumnFTable(clientContext->getMemoryManager());
    return std::make_unique<AttachDatabase>(std::move(info), std::move(messageTable),
        getOperatorID(), std::move(printInfo));
}

std::unique_ptr<PhysicalOperator> PlanMapper::mapDetachDatabase(
    const LogicalOperator* logicalOperator) {
    auto detachDatabase = logicalOperator->constPtrCast<LogicalDetachDatabase>();
    auto printInfo = std::make_unique<OPPrintInfo>();
    auto messageTable =
        FactorizedTableUtils::getSingleStringColumnFTable(clientContext->getMemoryManager());
    return std::make_unique<DetachDatabase>(detachDatabase->getDBName(), std::move(messageTable),
        getOperatorID(), std::move(printInfo));
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
    auto printInfo = std::make_unique<ExportDBPrintInfo>(filePath, boundFileInfo->options);
    auto messageTable =
        FactorizedTableUtils::getSingleStringColumnFTable(clientContext->getMemoryManager());
    auto exportDB = std::make_unique<ExportDB>(boundFileInfo->copy(),
        exportDatabase->isSchemaOnly(), messageTable, getOperatorID(), std::move(printInfo));
    auto sink = std::make_unique<DummySimpleSink>(messageTable, getOperatorID());
    for (auto child : exportDatabase->getChildren()) {
        sink->addChild(mapOperator(child.get()));
    }
    sink->addChild(std::move(exportDB));
    return sink;
}

std::unique_ptr<PhysicalOperator> PlanMapper::mapImportDatabase(
    const LogicalOperator* logicalOperator) {
    auto importDatabase = logicalOperator->constPtrCast<LogicalImportDatabase>();
    auto printInfo = std::make_unique<OPPrintInfo>();
    auto messageTable =
        FactorizedTableUtils::getSingleStringColumnFTable(clientContext->getMemoryManager());
    return std::make_unique<ImportDB>(importDatabase->getQuery(), importDatabase->getIndexQuery(),
        std::move(messageTable), getOperatorID(), std::move(printInfo));
}

std::unique_ptr<PhysicalOperator> PlanMapper::mapExtension(const LogicalOperator* logicalOperator) {
    auto logicalExtension = logicalOperator->constPtrCast<LogicalExtension>();
    auto& auxInfo = logicalExtension->getAuxInfo();
    auto path = auxInfo.path;
    auto messageTable =
        FactorizedTableUtils::getSingleStringColumnFTable(clientContext->getMemoryManager());
    switch (auxInfo.action) {
    case ExtensionAction::INSTALL: {
        auto installAuxInfo = auxInfo.contCast<InstallExtensionAuxInfo>();
        InstallExtensionInfo info{path, installAuxInfo.extensionRepo, installAuxInfo.forceInstall};
        auto printInfo = std::make_unique<InstallExtensionPrintInfo>(path);
        return std::make_unique<InstallExtension>(std::move(info), std::move(messageTable),
            getOperatorID(), std::move(printInfo));
    }
    case ExtensionAction::UNINSTALL: {
        auto printInfo = std::make_unique<UninstallExtensionPrintInfo>(path);
        return std::make_unique<UninstallExtension>(path, std::move(messageTable), getOperatorID(),
            std::move(printInfo));
    }
    case ExtensionAction::LOAD: {
        auto printInfo = std::make_unique<LoadExtensionPrintInfo>(path);
        return std::make_unique<LoadExtension>(path, std::move(messageTable), getOperatorID(),
            std::move(printInfo));
    }
    default:
        KU_UNREACHABLE;
    }
}

} // namespace processor
} // namespace kuzu
