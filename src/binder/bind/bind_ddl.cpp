#include "binder/binder.h"
#include "binder/ddl/bound_alter.h"
#include "binder/ddl/bound_create_table.h"
#include "binder/ddl/bound_drop_table.h"
#include "catalog/catalog.h"
#include "catalog/catalog_entry/node_table_catalog_entry.h"
#include "catalog/catalog_entry/rdf_graph_catalog_entry.h"
#include "catalog/catalog_entry/rel_group_catalog_entry.h"
#include "catalog/catalog_entry/rel_table_catalog_entry.h"
#include "common/exception/binder.h"
#include "common/exception/message.h"
#include "common/string_format.h"
#include "common/types/types.h"
#include "parser/ddl/alter.h"
#include "parser/ddl/create_table.h"
#include "parser/ddl/create_table_info.h"
#include "parser/ddl/drop.h"

using namespace kuzu::common;
using namespace kuzu::parser;
using namespace kuzu::catalog;

namespace kuzu {
namespace binder {

static void validateUniquePropertyName(const std::vector<PropertyInfo>& infos) {
    std::unordered_set<std::string> nameSet;
    for (auto& info : infos) {
        if (nameSet.contains(info.name)) {
            throw BinderException(
                stringFormat("Duplicated column name: {}, column name must be unique.", info.name));
        }
        nameSet.insert(info.name);
    }
}

std::vector<PropertyInfo> Binder::bindPropertyInfo(
    const std::vector<std::pair<std::string, std::string>>& propertyNameDataTypes) {
    std::vector<PropertyInfo> propertyInfos;
    propertyInfos.reserve(propertyNameDataTypes.size());
    for (auto& propertyNameDataType : propertyNameDataTypes) {
        propertyInfos.emplace_back(propertyNameDataType.first,
            LogicalType::fromString(propertyNameDataType.second));
    }
    validateUniquePropertyName(propertyInfos);
    for (auto& info : propertyInfos) {
        if (isReservedPropertyName(info.name)) {
            throw BinderException(stringFormat("{} is a reserved property name.", info.name));
        }
    }
    return propertyInfos;
}

static uint32_t bindPrimaryKey(const std::string& pkColName,
    const std::vector<PropertyInfo>& infos) {
    uint32_t primaryKeyIdx = UINT32_MAX;
    for (auto i = 0u; i < infos.size(); i++) {
        if (infos[i].name == pkColName) {
            primaryKeyIdx = i;
        }
    }
    if (primaryKeyIdx == UINT32_MAX) {
        throw BinderException(
            "Primary key " + pkColName + " does not match any of the predefined node properties.");
    }
    auto pkType = infos[primaryKeyIdx].type;
    switch (pkType.getPhysicalType()) {
    case PhysicalTypeID::UINT8:
    case PhysicalTypeID::UINT16:
    case PhysicalTypeID::UINT32:
    case PhysicalTypeID::UINT64:
    case PhysicalTypeID::INT8:
    case PhysicalTypeID::INT16:
    case PhysicalTypeID::INT32:
    case PhysicalTypeID::INT64:
    case PhysicalTypeID::INT128:
    case PhysicalTypeID::STRING:
    case PhysicalTypeID::FLOAT:
    case PhysicalTypeID::DOUBLE:
        break;
    default:
        throw BinderException(ExceptionMessage::invalidPKType(pkType.toString()));
    }
    return primaryKeyIdx;
}

BoundCreateTableInfo Binder::bindCreateTableInfo(const parser::CreateTableInfo* info) {
    switch (info->tableType) {
    case TableType::NODE: {
        return bindCreateNodeTableInfo(info);
    }
    case TableType::REL: {
        return bindCreateRelTableInfo(info);
    }
    case TableType::REL_GROUP: {
        return bindCreateRelTableGroupInfo(info);
    }
    case TableType::RDF: {
        return bindCreateRdfGraphInfo(info);
    }
    default: {
        KU_UNREACHABLE;
    }
    }
}

BoundCreateTableInfo Binder::bindCreateNodeTableInfo(const CreateTableInfo* info) {
    auto propertyInfos = bindPropertyInfo(info->propertyNameDataTypes);
    auto extraInfo = ku_dynamic_cast<const ExtraCreateTableInfo*, const ExtraCreateNodeTableInfo*>(
        info->extraInfo.get());
    auto primaryKeyIdx = bindPrimaryKey(extraInfo->pKName, propertyInfos);
    for (auto i = 0u; i < propertyInfos.size(); ++i) {
        if (propertyInfos[i].type == *LogicalType::SERIAL() && primaryKeyIdx != i) {
            throw BinderException("Serial property in node table must be the primary key.");
        }
    }
    auto boundExtraInfo =
        std::make_unique<BoundExtraCreateNodeTableInfo>(primaryKeyIdx, std::move(propertyInfos));
    return BoundCreateTableInfo(TableType::NODE, info->tableName, std::move(boundExtraInfo));
}

BoundCreateTableInfo Binder::bindCreateRelTableInfo(const CreateTableInfo* info) {
    std::vector<PropertyInfo> propertyInfos;
    propertyInfos.emplace_back(InternalKeyword::ID, *LogicalType::INTERNAL_ID());
    for (auto& propertyInfo : bindPropertyInfo(info->propertyNameDataTypes)) {
        propertyInfos.push_back(propertyInfo.copy());
    }
    for (auto& propertyInfo : propertyInfos) {
        if (propertyInfo.type.getLogicalTypeID() == LogicalTypeID::SERIAL) {
            throw BinderException("SERIAL properties are not supported in rel tables.");
        }
    }
    auto extraInfo = (ExtraCreateRelTableInfo*)info->extraInfo.get();
    auto srcMultiplicity = RelMultiplicityUtils::getFwd(extraInfo->relMultiplicity);
    auto dstMultiplicity = RelMultiplicityUtils::getBwd(extraInfo->relMultiplicity);
    auto srcTableID = bindTableID(extraInfo->srcTableName);
    validateTableType(srcTableID, TableType::NODE);
    auto dstTableID = bindTableID(extraInfo->dstTableName);
    validateTableType(dstTableID, TableType::NODE);
    auto boundExtraInfo = std::make_unique<BoundExtraCreateRelTableInfo>(srcMultiplicity,
        dstMultiplicity, srcTableID, dstTableID, std::move(propertyInfos));
    return BoundCreateTableInfo(TableType::REL, info->tableName, std::move(boundExtraInfo));
}

BoundCreateTableInfo Binder::bindCreateRelTableGroupInfo(const CreateTableInfo* info) {
    auto relGroupName = info->tableName;
    auto extraInfo = (ExtraCreateRelTableGroupInfo*)info->extraInfo.get();
    auto relMultiplicity = extraInfo->relMultiplicity;
    std::vector<BoundCreateTableInfo> boundCreateRelTableInfos;
    for (auto& [srcTableName, dstTableName] : extraInfo->srcDstTablePairs) {
        auto relTableName = std::string(relGroupName)
                                .append("_")
                                .append(srcTableName)
                                .append("_")
                                .append(dstTableName);
        auto relCreateInfo = std::make_unique<CreateTableInfo>(TableType::REL, relTableName);
        relCreateInfo->propertyNameDataTypes = info->propertyNameDataTypes;
        relCreateInfo->extraInfo =
            std::make_unique<ExtraCreateRelTableInfo>(relMultiplicity, srcTableName, dstTableName);
        boundCreateRelTableInfos.push_back(bindCreateRelTableInfo(relCreateInfo.get()));
    }
    auto boundExtraInfo =
        std::make_unique<BoundExtraCreateRelTableGroupInfo>(std::move(boundCreateRelTableInfos));
    return BoundCreateTableInfo(TableType::REL_GROUP, info->tableName, std::move(boundExtraInfo));
}

std::unique_ptr<BoundStatement> Binder::bindCreateTable(const Statement& statement) {
    auto& createTable = ku_dynamic_cast<const Statement&, const CreateTable&>(statement);
    auto tableName = createTable.getInfo()->tableName;
    if (clientContext->getCatalog()->containsTable(clientContext->getTx(), tableName)) {
        throw BinderException(tableName + " already exists in catalog.");
    }
    auto boundCreateInfo = bindCreateTableInfo(createTable.getInfo());
    return std::make_unique<BoundCreateTable>(std::move(boundCreateInfo));
}

std::unique_ptr<BoundStatement> Binder::bindDropTable(const Statement& statement) {
    auto& dropTable = ku_dynamic_cast<const Statement&, const Drop&>(statement);
    auto tableName = dropTable.getTableName();
    validateTableExist(tableName);
    auto catalog = clientContext->getCatalog();
    auto tableID = catalog->getTableID(clientContext->getTx(), tableName);
    auto tableEntry = catalog->getTableCatalogEntry(clientContext->getTx(), tableID);
    switch (tableEntry->getTableType()) {
    case TableType::NODE: {
        // Check node table is not referenced by rel table.
        for (auto& relTableEntry : catalog->getRelTableEntries(clientContext->getTx())) {
            if (relTableEntry->isParent(tableID)) {
                throw BinderException(stringFormat("Cannot delete node table {} because it is "
                                                   "referenced by relationship table {}.",
                    tableEntry->getName(), relTableEntry->getName()));
            }
        }
        // Check node table is not referenced by rdf graph
        for (auto& rdfEntry : catalog->getRdfGraphEntries(clientContext->getTx())) {
            if (rdfEntry->isParent(tableID)) {
                throw BinderException(stringFormat(
                    "Cannot delete node table {} because it is referenced by rdfGraph {}.",
                    tableName, rdfEntry->getName()));
            }
        }
    } break;
    case TableType::REL: {
        // Check rel table is not referenced by rel group.
        for (auto& relTableGroupEntry : catalog->getRelTableGroupEntries(clientContext->getTx())) {
            if (relTableGroupEntry->isParent(tableID)) {
                throw BinderException(stringFormat("Cannot delete relationship table {} because it "
                                                   "is referenced by relationship group {}.",
                    tableName, relTableGroupEntry->getName()));
            }
        }
        // Check rel table is not referenced by rdf graph.
        for (auto& rdfGraphEntry : catalog->getRdfGraphEntries(clientContext->getTx())) {
            if (rdfGraphEntry->isParent(tableID)) {
                throw BinderException(stringFormat(
                    "Cannot delete relationship table {} because it is referenced by rdfGraph {}.",
                    tableName, rdfGraphEntry->getName()));
            }
        }
    } break;
    case TableType::RDF: {
        auto rdfGraphEntry = ku_dynamic_cast<TableCatalogEntry*, RDFGraphCatalogEntry*>(tableEntry);
        // Check resource table is not referenced by rel table other than its triple table.
        for (auto& relTableEntry : catalog->getRelTableEntries(clientContext->getTx())) {
            if (relTableEntry->getTableID() == rdfGraphEntry->getResourceTripleTableID() ||
                relTableEntry->getTableID() == rdfGraphEntry->getLiteralTripleTableID()) {
                continue;
            }
            if (relTableEntry->isParent(rdfGraphEntry->getResourceTableID())) {
                throw BinderException(stringFormat("Cannot delete rdfGraph {} because its resource "
                                                   "table is referenced by relationship table {}.",
                    tableEntry->getName(), relTableEntry->getName()));
            }
        }
        // Check literal table is not referenced by rel table other than its triple table.
        for (auto& relTableEntry : catalog->getRelTableEntries(clientContext->getTx())) {
            if (relTableEntry->getTableID() == rdfGraphEntry->getLiteralTripleTableID()) {
                continue;
            }
            if (relTableEntry->isParent(rdfGraphEntry->getLiteralTableID())) {
                throw BinderException(stringFormat("Cannot delete rdfGraph {} because its literal "
                                                   "table is referenced by relationship table {}.",
                    tableEntry->getName(), relTableEntry->getName()));
            }
        }
    } break;
    default:
        break;
    }
    return make_unique<BoundDropTable>(tableID, tableName);
}

std::unique_ptr<BoundStatement> Binder::bindAlter(const Statement& statement) {
    auto& alter = ku_dynamic_cast<const Statement&, const Alter&>(statement);
    auto catalog = clientContext->getCatalog();
    auto tableID = catalog->getTableID(clientContext->getTx(), alter.getInfo()->tableName);
    for (auto& schema : catalog->getRdfGraphEntries(clientContext->getTx())) {
        if (schema->isParent(tableID)) {
            throw BinderException(
                stringFormat("Cannot alter table {} because it is referenced by rdfGraph {}.",
                    alter.getInfo()->tableName, schema->getName()));
        }
    }
    switch (alter.getInfo()->type) {
    case AlterType::RENAME_TABLE: {
        return bindRenameTable(statement);
    }
    case AlterType::ADD_PROPERTY: {
        return bindAddProperty(statement);
    }
    case AlterType::DROP_PROPERTY: {
        return bindDropProperty(statement);
    }
    case AlterType::RENAME_PROPERTY: {
        return bindRenameProperty(statement);
    }
    default: {
        KU_UNREACHABLE;
    }
    }
}

std::unique_ptr<BoundStatement> Binder::bindRenameTable(const Statement& statement) {
    auto& alter = ku_dynamic_cast<const Statement&, const Alter&>(statement);
    auto info = alter.getInfo();
    auto extraInfo = ku_dynamic_cast<ExtraAlterInfo*, ExtraRenameTableInfo*>(info->extraInfo.get());
    auto tableName = info->tableName;
    auto newName = extraInfo->newName;
    validateTableExist(tableName);
    auto catalog = clientContext->getCatalog();
    if (catalog->containsTable(clientContext->getTx(), newName)) {
        throw BinderException("Table: " + newName + " already exists.");
    }
    auto tableID = catalog->getTableID(clientContext->getTx(), tableName);
    auto boundExtraInfo = std::make_unique<BoundExtraRenameTableInfo>(newName);
    auto boundInfo =
        BoundAlterInfo(AlterType::RENAME_TABLE, tableName, tableID, std::move(boundExtraInfo));
    return std::make_unique<BoundAlter>(std::move(boundInfo));
}

static void validatePropertyExist(TableCatalogEntry* tableEntry, const std::string& propertyName) {
    if (!tableEntry->containProperty(propertyName)) {
        throw BinderException(
            tableEntry->getName() + " table does not have property " + propertyName + ".");
    }
}

static void validatePropertyNotExist(TableCatalogEntry* tableEntry,
    const std::string& propertyName) {
    if (tableEntry->containProperty(propertyName)) {
        throw BinderException(
            tableEntry->getName() + " table already has property " + propertyName + ".");
    }
}

static void validatePropertyDDLOnTable(TableCatalogEntry* tableEntry,
    const std::string& ddlOperation) {
    switch (tableEntry->getTableType()) {
    case TableType::REL_GROUP:
    case TableType::RDF: {
        throw BinderException(
            stringFormat("Cannot {} property on table {} with type {}.", ddlOperation,
                tableEntry->getName(), TableTypeUtils::toString(tableEntry->getTableType())));
    }
    default:
        return;
    }
}

std::unique_ptr<BoundStatement> Binder::bindAddProperty(const Statement& statement) {
    auto& alter = ku_dynamic_cast<const Statement&, const Alter&>(statement);
    auto info = alter.getInfo();
    auto extraInfo = ku_dynamic_cast<ExtraAlterInfo*, ExtraAddPropertyInfo*>(info->extraInfo.get());
    auto tableName = info->tableName;
    auto dataType = LogicalType::fromString(extraInfo->dataType);
    auto propertyName = extraInfo->propertyName;
    validateTableExist(tableName);
    auto catalog = clientContext->getCatalog();
    auto tableID = catalog->getTableID(clientContext->getTx(), tableName);
    auto tableSchema = catalog->getTableCatalogEntry(clientContext->getTx(), tableID);
    validatePropertyDDLOnTable(tableSchema, "add");
    validatePropertyNotExist(tableSchema, propertyName);
    if (dataType.getLogicalTypeID() == LogicalTypeID::SERIAL) {
        throw BinderException("Serial property in node table must be the primary key.");
    }
    auto defaultVal = expressionBinder.implicitCastIfNecessary(
        expressionBinder.bindExpression(*extraInfo->defaultValue), dataType);
    auto boundExtraInfo =
        std::make_unique<BoundExtraAddPropertyInfo>(propertyName, dataType, std::move(defaultVal));
    auto boundInfo =
        BoundAlterInfo(AlterType::ADD_PROPERTY, tableName, tableID, std::move(boundExtraInfo));
    return std::make_unique<BoundAlter>(std::move(boundInfo));
}

std::unique_ptr<BoundStatement> Binder::bindDropProperty(const Statement& statement) {
    auto& alter = ku_dynamic_cast<const Statement&, const Alter&>(statement);
    auto info = alter.getInfo();
    auto extraInfo =
        ku_dynamic_cast<ExtraAlterInfo*, ExtraDropPropertyInfo*>(info->extraInfo.get());
    auto tableName = info->tableName;
    auto propertyName = extraInfo->propertyName;
    validateTableExist(tableName);
    auto catalog = clientContext->getCatalog();
    auto tableID = catalog->getTableID(clientContext->getTx(), tableName);
    auto tableEntry = catalog->getTableCatalogEntry(clientContext->getTx(), tableID);
    validatePropertyDDLOnTable(tableEntry, "drop");
    validatePropertyExist(tableEntry, propertyName);
    auto propertyID = tableEntry->getPropertyID(propertyName);
    if (tableEntry->getTableType() == TableType::NODE &&
        ku_dynamic_cast<TableCatalogEntry*, NodeTableCatalogEntry*>(tableEntry)
                ->getPrimaryKeyPID() == propertyID) {
        throw BinderException("Cannot drop primary key of a node table.");
    }
    auto boundExtraInfo = std::make_unique<BoundExtraDropPropertyInfo>(propertyID);
    auto boundInfo =
        BoundAlterInfo(AlterType::DROP_PROPERTY, tableName, tableID, std::move(boundExtraInfo));
    return std::make_unique<BoundAlter>(std::move(boundInfo));
}

std::unique_ptr<BoundStatement> Binder::bindRenameProperty(const Statement& statement) {
    auto& alter = ku_dynamic_cast<const Statement&, const Alter&>(statement);
    auto info = alter.getInfo();
    auto extraInfo =
        ku_dynamic_cast<ExtraAlterInfo*, ExtraRenamePropertyInfo*>(info->extraInfo.get());
    auto tableName = info->tableName;
    auto propertyName = extraInfo->propertyName;
    auto newName = extraInfo->newName;
    validateTableExist(tableName);
    auto catalog = clientContext->getCatalog();
    auto tableID = catalog->getTableID(clientContext->getTx(), tableName);
    auto tableSchema = catalog->getTableCatalogEntry(clientContext->getTx(), tableID);
    validatePropertyDDLOnTable(tableSchema, "rename");
    validatePropertyExist(tableSchema, propertyName);
    auto propertyID = tableSchema->getPropertyID(propertyName);
    validatePropertyNotExist(tableSchema, newName);
    auto boundExtraInfo = std::make_unique<BoundExtraRenamePropertyInfo>(propertyID, newName);
    auto boundInfo =
        BoundAlterInfo(AlterType::RENAME_PROPERTY, tableName, tableID, std::move(boundExtraInfo));
    return std::make_unique<BoundAlter>(std::move(boundInfo));
}

} // namespace binder
} // namespace kuzu
