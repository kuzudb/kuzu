#include "catalog/catalog.h"

#include "catalog/rel_table_group_schema.h"
#include "storage/wal/wal.h"
#include "transaction/transaction_action.h"

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
            catalogContentForWriteTrx->saveToFile(
                wal->getDirectory(), FileVersionType::WAL_VERSION);
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

table_id_t Catalog::addNodeTableSchema(const binder::BoundCreateTableInfo& info) {
    initCatalogContentForWriteTrxIfNecessary();
    return catalogContentForWriteTrx->addNodeTableSchema(info);
}

table_id_t Catalog::addRelTableSchema(const binder::BoundCreateTableInfo& info) {
    initCatalogContentForWriteTrxIfNecessary();
    auto tableID = catalogContentForWriteTrx->addRelTableSchema(info);
    return tableID;
}

common::table_id_t Catalog::addRelTableGroupSchema(const binder::BoundCreateTableInfo& info) {
    initCatalogContentForWriteTrxIfNecessary();
    auto tableID = catalogContentForWriteTrx->addRelTableGroupSchema(info);
    return tableID;
}

table_id_t Catalog::addRdfGraphSchema(const binder::BoundCreateTableInfo& info) {
    initCatalogContentForWriteTrxIfNecessary();
    return catalogContentForWriteTrx->addRdfGraphSchema(info);
}

void Catalog::dropTableSchema(table_id_t tableID) {
    initCatalogContentForWriteTrxIfNecessary();
    auto tableSchema = catalogContentForWriteTrx->getTableSchema(tableID);
    switch (tableSchema->tableType) {
    case TableType::REL_GROUP: {
        auto relTableGroupSchema = reinterpret_cast<RelTableGroupSchema*>(tableSchema);
        auto relTableIDs = relTableGroupSchema->getRelTableIDs();
        catalogContentForWriteTrx->dropTableSchema(tableID);
        for (auto relTableID : relTableIDs) {
            wal->logDropTableRecord(relTableID);
        }
    } break;
    default: {
        catalogContentForWriteTrx->dropTableSchema(tableID);
        wal->logDropTableRecord(tableID);
    }
    }
}

void Catalog::renameTable(table_id_t tableID, const std::string& newName) {
    initCatalogContentForWriteTrxIfNecessary();
    catalogContentForWriteTrx->renameTable(tableID, newName);
}

void Catalog::addNodeProperty(
    table_id_t tableID, const std::string& propertyName, std::unique_ptr<LogicalType> dataType) {
    initCatalogContentForWriteTrxIfNecessary();
    catalogContentForWriteTrx->getTableSchema(tableID)->addNodeProperty(
        propertyName, std::move(dataType));
}

void Catalog::addRelProperty(
    table_id_t tableID, const std::string& propertyName, std::unique_ptr<LogicalType> dataType) {
    initCatalogContentForWriteTrxIfNecessary();
    catalogContentForWriteTrx->getTableSchema(tableID)->addRelProperty(
        propertyName, std::move(dataType));
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

void Catalog::addFunction(std::string name, function::function_set functionSet) {
    catalogContentForReadOnlyTrx->addFunction(std::move(name), std::move(functionSet));
}

void Catalog::addScalarMacroFunction(
    std::string name, std::unique_ptr<function::ScalarMacroFunction> macro) {
    initCatalogContentForWriteTrxIfNecessary();
    catalogContentForWriteTrx->addScalarMacroFunction(std::move(name), std::move(macro));
}

void Catalog::setTableComment(table_id_t tableID, const std::string& comment) {
    initCatalogContentForWriteTrxIfNecessary();
    catalogContentForWriteTrx->getTableSchema(tableID)->setComment(comment);
}

} // namespace catalog
} // namespace kuzu
