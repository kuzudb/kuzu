#include "catalog/catalog.h"

#include "common/ser_deser.h"
#include "common/string_utils.h"
#include "storage/storage_utils.h"

using namespace kuzu::common;
using namespace kuzu::storage;
using namespace kuzu::transaction;

namespace kuzu {
namespace catalog {

Catalog::Catalog() : wal{nullptr} {
    catalogContentForReadOnlyTrx = std::make_unique<CatalogContent>();
}

Catalog::Catalog(WAL* wal) : wal{wal} {
    catalogContentForReadOnlyTrx = std::make_unique<CatalogContent>(wal->getDirectory());
}

void Catalog::prepareCommitOrRollback(TransactionAction action) {
    if (hasUpdates()) {
        wal->logCatalogRecord();
        if (action == TransactionAction::COMMIT) {
            catalogContentForWriteTrx->saveToFile(wal->getDirectory(), DBFileType::WAL_VERSION);
        }
    }
}

void Catalog::checkpointInMemory() {
    if (hasUpdates()) {
        catalogContentForReadOnlyTrx = std::move(catalogContentForWriteTrx);
    }
}

ExpressionType Catalog::getFunctionType(const std::string& name) const {
    return catalogContentForReadOnlyTrx->getFunctionType(name);
}

table_id_t Catalog::addNodeTableSchema(std::string tableName, property_id_t primaryKeyId,
    std::vector<std::unique_ptr<Property>> propertyDefinitions) {
    initCatalogContentForWriteTrxIfNecessary();
    auto tableID = catalogContentForWriteTrx->addNodeTableSchema(
        std::move(tableName), primaryKeyId, std::move(propertyDefinitions));
    wal->logNodeTableRecord(tableID);
    return tableID;
}

table_id_t Catalog::addRelTableSchema(std::string tableName, RelMultiplicity relMultiplicity,
    std::vector<std::unique_ptr<Property>> propertyDefinitions, table_id_t srcTableID,
    table_id_t dstTableID, std::unique_ptr<LogicalType> srcPKDataType,
    std::unique_ptr<LogicalType> dstPKDataType) {
    initCatalogContentForWriteTrxIfNecessary();
    auto tableID = catalogContentForWriteTrx->addRelTableSchema(std::move(tableName),
        relMultiplicity, std::move(propertyDefinitions), srcTableID, dstTableID,
        std::move(srcPKDataType), std::move(dstPKDataType));
    wal->logRelTableRecord(tableID);
    return tableID;
}

void Catalog::dropTableSchema(table_id_t tableID) {
    initCatalogContentForWriteTrxIfNecessary();
    catalogContentForWriteTrx->dropTableSchema(tableID);
    wal->logDropTableRecord(tableID);
}

void Catalog::renameTable(table_id_t tableID, const std::string& newName) {
    initCatalogContentForWriteTrxIfNecessary();
    catalogContentForWriteTrx->renameTable(tableID, newName);
}

void Catalog::addNodeProperty(table_id_t tableID, const std::string& propertyName,
    std::unique_ptr<LogicalType> dataType, std::unique_ptr<MetadataDAHInfo> metadataDAHInfo) {
    initCatalogContentForWriteTrxIfNecessary();
    catalogContentForWriteTrx->getTableSchema(tableID)->addNodeProperty(
        propertyName, std::move(dataType), std::move(metadataDAHInfo));
    wal->logAddPropertyRecord(
        tableID, catalogContentForWriteTrx->getTableSchema(tableID)->getPropertyID(propertyName));
}

void Catalog::addRelProperty(
    table_id_t tableID, const std::string& propertyName, std::unique_ptr<LogicalType> dataType) {
    initCatalogContentForWriteTrxIfNecessary();
    catalogContentForWriteTrx->getTableSchema(tableID)->addRelProperty(
        propertyName, std::move(dataType));
    wal->logAddPropertyRecord(
        tableID, catalogContentForWriteTrx->getTableSchema(tableID)->getPropertyID(propertyName));
}

void Catalog::dropProperty(table_id_t tableID, property_id_t propertyID) {
    initCatalogContentForWriteTrxIfNecessary();
    catalogContentForWriteTrx->getTableSchema(tableID)->dropProperty(propertyID);
    wal->logDropPropertyRecord(tableID, propertyID);
}

void Catalog::renameProperty(
    table_id_t tableID, property_id_t propertyID, const std::string& newName) {
    initCatalogContentForWriteTrxIfNecessary();
    catalogContentForWriteTrx->getTableSchema(tableID)->renameProperty(propertyID, newName);
}

std::unordered_set<RelTableSchema*> Catalog::getAllRelTableSchemasContainBoundTable(
    table_id_t boundTableID) const {
    std::unordered_set<RelTableSchema*> relTableSchemas;
    auto nodeTableSchema = getReadOnlyVersion()->getNodeTableSchema(boundTableID);
    for (auto& fwdRelTableID : nodeTableSchema->getFwdRelTableIDSet()) {
        relTableSchemas.insert(getReadOnlyVersion()->getRelTableSchema(fwdRelTableID));
    }
    for (auto& bwdRelTableID : nodeTableSchema->getBwdRelTableIDSet()) {
        relTableSchemas.insert(getReadOnlyVersion()->getRelTableSchema(bwdRelTableID));
    }
    return relTableSchemas;
}

void Catalog::addVectorFunction(
    std::string name, function::vector_function_definitions definitions) {
    catalogContentForReadOnlyTrx->addVectorFunction(std::move(name), std::move(definitions));
}

void Catalog::addScalarMacroFunction(
    std::string name, std::unique_ptr<function::ScalarMacroFunction> macro) {
    initCatalogContentForWriteTrxIfNecessary();
    catalogContentForWriteTrx->addScalarMacroFunction(std::move(name), std::move(macro));
}

} // namespace catalog
} // namespace kuzu
