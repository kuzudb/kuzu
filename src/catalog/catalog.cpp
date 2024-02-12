#include "catalog/catalog.h"

#include "catalog/rdf_graph_schema.h"
#include "catalog/rel_table_group_schema.h"
#include "storage/wal/wal.h"
#include "transaction/transaction.h"
#include "transaction/transaction_action.h"

using namespace kuzu::common;
using namespace kuzu::storage;
using namespace kuzu::transaction;

namespace kuzu {
namespace catalog {

Catalog::Catalog(VirtualFileSystem* vfs) : isUpdated{false}, wal{nullptr} {
    readOnlyVersion = std::make_unique<CatalogContent>(vfs);
}

Catalog::Catalog(WAL* wal, VirtualFileSystem* vfs) : isUpdated{false}, wal{wal} {
    readOnlyVersion = std::make_unique<CatalogContent>(wal->getDirectory(), vfs);
}

uint64_t Catalog::getTableCount(Transaction* tx) const {
    return getVersion(tx)->getTableCount();
}

bool Catalog::containsNodeTable(Transaction* tx) const {
    return getVersion(tx)->containsTable(TableType::NODE);
}

bool Catalog::containsRelTable(Transaction* tx) const {
    return getVersion(tx)->containsTable(TableType::REL);
}

bool Catalog::containsRdfGraph(Transaction* tx) const {
    return getVersion(tx)->containsTable(TableType::RDF);
}

bool Catalog::containsTable(Transaction* tx, const std::string& tableName) const {
    return getVersion(tx)->containsTable(tableName);
}

table_id_t Catalog::getTableID(Transaction* tx, const std::string& tableName) const {
    return getVersion(tx)->getTableID(tableName);
}

std::vector<common::table_id_t> Catalog::getNodeTableIDs(Transaction* tx) const {
    return getVersion(tx)->getTableIDs(TableType::NODE);
}

std::vector<common::table_id_t> Catalog::getRelTableIDs(Transaction* tx) const {
    return getVersion(tx)->getTableIDs(TableType::REL);
}

std::vector<common::table_id_t> Catalog::getRdfGraphIDs(Transaction* tx) const {
    return getVersion(tx)->getTableIDs(TableType::RDF);
}

std::string Catalog::getTableName(Transaction* tx, table_id_t tableID) const {
    return getVersion(tx)->getTableName(tableID);
}

TableSchema* Catalog::getTableSchema(Transaction* tx, table_id_t tableID) const {
    return getVersion(tx)->getTableSchema(tableID);
}

std::vector<TableSchema*> Catalog::getNodeTableSchemas(Transaction* tx) const {
    return getVersion(tx)->getTableSchemas(TableType::NODE);
}

std::vector<TableSchema*> Catalog::getRelTableSchemas(Transaction* tx) const {
    return getVersion(tx)->getTableSchemas(TableType::REL);
}

std::vector<TableSchema*> Catalog::getRelTableGroupSchemas(Transaction* tx) const {
    return getVersion(tx)->getTableSchemas(TableType::REL_GROUP);
}

std::vector<TableSchema*> Catalog::getRdfGraphSchemas(transaction::Transaction* tx) const {
    return getVersion(tx)->getTableSchemas(TableType::RDF);
}

std::vector<TableSchema*> Catalog::getTableSchemas(Transaction* tx) const {
    std::vector<TableSchema*> result;
    for (auto& [_, schema] : getVersion(tx)->tableSchemas) {
        result.push_back(schema.get());
    }
    return result;
}

std::vector<TableSchema*> Catalog::getTableSchemas(
    Transaction* tx, const table_id_vector_t& tableIDs) const {
    std::vector<TableSchema*> result;
    for (auto tableID : tableIDs) {
        result.push_back(getVersion(tx)->getTableSchema(tableID));
    }
    return result;
}

void Catalog::prepareCommitOrRollback(TransactionAction action) {
    if (hasUpdates()) {
        wal->logCatalogRecord();
        if (action == TransactionAction::COMMIT) {
            readWriteVersion->saveToFile(wal->getDirectory(), FileVersionType::WAL_VERSION);
        }
    }
}

void Catalog::checkpointInMemory() {
    if (hasUpdates()) {
        readOnlyVersion = std::move(readWriteVersion);
        resetToNotUpdated();
    }
}

ExpressionType Catalog::getFunctionType(Transaction* tx, const std::string& name) const {
    return getVersion(tx)->getFunctionType(name);
}

table_id_t Catalog::addNodeTableSchema(const binder::BoundCreateTableInfo& info) {
    KU_ASSERT(readWriteVersion != nullptr);
    setToUpdated();
    return readWriteVersion->addNodeTableSchema(info);
}

table_id_t Catalog::addRelTableSchema(const binder::BoundCreateTableInfo& info) {
    KU_ASSERT(readWriteVersion != nullptr);
    setToUpdated();
    return readWriteVersion->addRelTableSchema(info);
}

common::table_id_t Catalog::addRelTableGroupSchema(const binder::BoundCreateTableInfo& info) {
    KU_ASSERT(readWriteVersion != nullptr);
    setToUpdated();
    auto tableID = readWriteVersion->addRelTableGroupSchema(info);
    return tableID;
}

table_id_t Catalog::addRdfGraphSchema(const binder::BoundCreateTableInfo& info) {
    KU_ASSERT(readWriteVersion != nullptr);
    setToUpdated();
    return readWriteVersion->addRdfGraphSchema(info);
}

void Catalog::dropTableSchema(table_id_t tableID) {
    KU_ASSERT(readWriteVersion != nullptr);
    setToUpdated();
    auto tableSchema = readWriteVersion->getTableSchema(tableID);
    switch (tableSchema->tableType) {
    case TableType::REL_GROUP: {
        auto relTableGroupSchema = ku_dynamic_cast<TableSchema*, RelTableGroupSchema*>(tableSchema);
        auto relTableIDs = relTableGroupSchema->getRelTableIDs();
        readWriteVersion->dropTableSchema(tableID);
        for (auto relTableID : relTableIDs) {
            wal->logDropTableRecord(relTableID);
        }
    } break;
    case TableType::RDF: {
        auto rdfGraphSchema = ku_dynamic_cast<TableSchema*, RdfGraphSchema*>(tableSchema);
        auto graphID = rdfGraphSchema->getTableID();
        auto rtTableID = rdfGraphSchema->getResourceTripleTableID();
        auto ltTableID = rdfGraphSchema->getLiteralTripleTableID();
        auto rTableID = rdfGraphSchema->getResourceTableID();
        auto lTableID = rdfGraphSchema->getLiteralTableID();
        readWriteVersion->dropTableSchema(rtTableID);
        readWriteVersion->dropTableSchema(ltTableID);
        readWriteVersion->dropTableSchema(rTableID);
        readWriteVersion->dropTableSchema(lTableID);
        readWriteVersion->dropTableSchema(graphID);
        wal->logDropTableRecord(rtTableID);
        wal->logDropTableRecord(ltTableID);
        wal->logDropTableRecord(rTableID);
        wal->logDropTableRecord(lTableID);
        wal->logDropTableRecord(graphID);
    } break;
    default: {
        readWriteVersion->dropTableSchema(tableID);
        wal->logDropTableRecord(tableID);
    }
    }
}

void Catalog::renameTable(table_id_t tableID, const std::string& newName) {
    KU_ASSERT(readWriteVersion != nullptr);
    setToUpdated();
    auto tableSchema = readWriteVersion->getTableSchema(tableID);
    switch (tableSchema->tableType) {
    case TableType::RDF: {
        auto rdfGraphSchema = ku_dynamic_cast<TableSchema*, RdfGraphSchema*>(tableSchema);
        readWriteVersion->renameTable(
            rdfGraphSchema->getResourceTableID(), RdfGraphSchema::getResourceTableName(newName));
        readWriteVersion->renameTable(
            rdfGraphSchema->getLiteralTableID(), RdfGraphSchema::getLiteralTableName(newName));
        readWriteVersion->renameTable(rdfGraphSchema->getResourceTripleTableID(),
            RdfGraphSchema::getResourceTripleTableName(newName));
        readWriteVersion->renameTable(rdfGraphSchema->getLiteralTripleTableID(),
            RdfGraphSchema::getLiteralTripleTableName(newName));
        readWriteVersion->renameTable(tableID, newName);
    } break;
    default: {
        readWriteVersion->renameTable(tableID, newName);
    }
    }
}

void Catalog::addNodeProperty(
    table_id_t tableID, const std::string& propertyName, std::unique_ptr<LogicalType> dataType) {
    KU_ASSERT(readWriteVersion != nullptr);
    setToUpdated();
    readWriteVersion->getTableSchema(tableID)->addProperty(propertyName, std::move(dataType));
}

void Catalog::addRelProperty(
    table_id_t tableID, const std::string& propertyName, std::unique_ptr<LogicalType> dataType) {
    KU_ASSERT(readWriteVersion != nullptr);
    setToUpdated();
    readWriteVersion->getTableSchema(tableID)->addProperty(propertyName, std::move(dataType));
}

void Catalog::dropProperty(table_id_t tableID, property_id_t propertyID) {
    KU_ASSERT(readWriteVersion != nullptr);
    setToUpdated();
    readWriteVersion->getTableSchema(tableID)->dropProperty(propertyID);
    wal->logDropPropertyRecord(tableID, propertyID);
}

void Catalog::renameProperty(
    table_id_t tableID, property_id_t propertyID, const std::string& newName) {
    KU_ASSERT(readWriteVersion != nullptr);
    setToUpdated();
    readWriteVersion->getTableSchema(tableID)->renameProperty(propertyID, newName);
}

void Catalog::addFunction(std::string name, function::function_set functionSet) {
    initCatalogContentForWriteTrxIfNecessary();
    KU_ASSERT(readWriteVersion != nullptr);
    setToUpdated();
    readWriteVersion->addFunction(std::move(name), std::move(functionSet));
}

void Catalog::addBuiltInFunction(std::string name, function::function_set functionSet) {
    readOnlyVersion->addFunction(std::move(name), std::move(functionSet));
}

bool Catalog::containsMacro(Transaction* tx, const std::string& macroName) const {
    return getVersion(tx)->containMacro(macroName);
}

void Catalog::addScalarMacroFunction(
    std::string name, std::unique_ptr<function::ScalarMacroFunction> macro) {
    KU_ASSERT(readWriteVersion != nullptr);
    setToUpdated();
    readWriteVersion->addScalarMacroFunction(std::move(name), std::move(macro));
}

std::vector<std::string> Catalog::getMacroNames(transaction::Transaction* tx) const {
    std::vector<std::string> macroNames;
    for (auto& macro : getVersion(tx)->macros) {
        macroNames.push_back(macro.first);
    }
    return macroNames;
}

void Catalog::setTableComment(table_id_t tableID, const std::string& comment) {
    KU_ASSERT(readWriteVersion != nullptr);
    setToUpdated();
    readWriteVersion->getTableSchema(tableID)->setComment(comment);
}

CatalogContent* Catalog::getVersion(Transaction* tx) const {
    return tx->getType() == TransactionType::READ_ONLY ? readOnlyVersion.get() :
                                                         readWriteVersion.get();
}

} // namespace catalog
} // namespace kuzu
