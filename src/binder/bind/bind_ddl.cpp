#include "binder/binder.h"
#include "binder/ddl/bound_alter.h"
#include "binder/ddl/bound_create_table.h"
#include "binder/ddl/bound_drop_table.h"
#include "catalog/node_table_schema.h"
#include "catalog/rdf_graph_schema.h"
#include "catalog/rel_table_schema.h"
#include "common/exception/binder.h"
#include "common/exception/message.h"
#include "common/string_format.h"
#include "common/types/types.h"
#include "main/client_context.h"
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

static void validateReservedPropertyName(const std::vector<PropertyInfo>& infos) {
    for (auto& info : infos) {
        if (TableSchema::isReservedPropertyName(info.name)) {
            throw BinderException(
                stringFormat("PropertyName: {} is an internal reserved propertyName.", info.name));
        }
    }
}

std::vector<PropertyInfo> Binder::bindPropertyInfo(
    const std::vector<std::pair<std::string, std::string>>& propertyNameDataTypes) {
    std::vector<PropertyInfo> propertyInfos;
    propertyInfos.reserve(propertyNameDataTypes.size());
    for (auto& propertyNameDataType : propertyNameDataTypes) {
        propertyInfos.emplace_back(
            propertyNameDataType.first, *bindDataType(propertyNameDataType.second));
    }
    validateUniquePropertyName(propertyInfos);
    validateReservedPropertyName(propertyInfos);
    return propertyInfos;
}

static uint32_t bindPrimaryKey(
    const std::string& pkColName, const std::vector<PropertyInfo>& infos) {
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
    auto boundExtraInfo = std::make_unique<BoundExtraCreateRelTableInfo>(
        srcMultiplicity, dstMultiplicity, srcTableID, dstTableID, std::move(propertyInfos));
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
    if (catalog.containsTable(clientContext->getTx(), tableName)) {
        throw BinderException(tableName + " already exists in catalog.");
    }
    auto boundCreateInfo = bindCreateTableInfo(createTable.getInfo());
    return std::make_unique<BoundCreateTable>(std::move(boundCreateInfo));
}

std::unique_ptr<BoundStatement> Binder::bindDropTable(const Statement& statement) {
    auto& dropTable = ku_dynamic_cast<const Statement&, const Drop&>(statement);
    auto tableName = dropTable.getTableName();
    validateTableExist(tableName);
    auto tableID = catalog.getTableID(clientContext->getTx(), tableName);
    auto tableSchema = catalog.getTableSchema(clientContext->getTx(), tableID);
    switch (tableSchema->tableType) {
    case TableType::NODE: {
        // Check node table is not referenced by rel table.
        for (auto& schema : catalog.getRelTableSchemas(clientContext->getTx())) {
            if (schema->isParent(tableID)) {
                throw BinderException(stringFormat("Cannot delete node table {} because it is "
                                                   "referenced by relationship table {}.",
                    tableSchema->getName(), schema->getName()));
            }
        }
        // Check node table is not referenced by rdf graph
        for (auto& schema : catalog.getRdfGraphSchemas(clientContext->getTx())) {
            if (schema->isParent(tableID)) {
                throw BinderException(stringFormat(
                    "Cannot delete node table {} because it is referenced by rdfGraph {}.",
                    tableName, schema->getName()));
            }
        }
    } break;
    case TableType::REL: {
        // Check rel table is not referenced by rel group.
        for (auto& schema : catalog.getRelTableGroupSchemas(clientContext->getTx())) {
            if (schema->isParent(tableID)) {
                throw BinderException(stringFormat("Cannot delete relationship table {} because it "
                                                   "is referenced by relationship group {}.",
                    tableName, schema->getName()));
            }
        }
        // Check rel table is not referenced by rdf graph.
        for (auto& schema : catalog.getRdfGraphSchemas(clientContext->getTx())) {
            if (schema->isParent(tableID)) {
                throw BinderException(stringFormat(
                    "Cannot delete relationship table {} because it is referenced by rdfGraph {}.",
                    tableName, schema->getName()));
            }
        }
    } break;
    case TableType::RDF: {
        auto rdfGraphSchema = ku_dynamic_cast<TableSchema*, RdfGraphSchema*>(tableSchema);
        // Check resource table is not referenced by rel table other than its triple table.
        for (auto& schema : catalog.getRelTableSchemas(clientContext->getTx())) {
            if (schema->getTableID() == rdfGraphSchema->getResourceTripleTableID() ||
                schema->getTableID() == rdfGraphSchema->getLiteralTripleTableID()) {
                continue;
            }
            if (schema->isParent(rdfGraphSchema->getResourceTableID())) {
                throw BinderException(stringFormat("Cannot delete rdfGraph {} because its resource "
                                                   "table is referenced by relationship table {}.",
                    tableSchema->getName(), schema->getName()));
            }
        }
        // Check literal table is not referenced by rel table other than its triple table.
        for (auto& schema : catalog.getRelTableSchemas(clientContext->getTx())) {
            if (schema->getTableID() == rdfGraphSchema->getLiteralTripleTableID()) {
                continue;
            }
            if (schema->isParent(rdfGraphSchema->getLiteralTableID())) {
                throw BinderException(stringFormat("Cannot delete rdfGraph {} because its literal "
                                                   "table is referenced by relationship table {}.",
                    tableSchema->getName(), schema->getName()));
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
    auto tableID = catalog.getTableID(clientContext->getTx(), alter.getInfo()->tableName);
    for (auto& schema : catalog.getRdfGraphSchemas(clientContext->getTx())) {
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
    if (catalog.containsTable(clientContext->getTx(), newName)) {
        throw BinderException("Table: " + newName + " already exists.");
    }
    auto tableID = catalog.getTableID(clientContext->getTx(), tableName);
    auto boundExtraInfo = std::make_unique<BoundExtraRenameTableInfo>(newName);
    auto boundInfo =
        BoundAlterInfo(AlterType::RENAME_TABLE, tableName, tableID, std::move(boundExtraInfo));
    return std::make_unique<BoundAlter>(std::move(boundInfo));
}

static void validatePropertyExist(TableSchema* tableSchema, const std::string& propertyName) {
    if (!tableSchema->containProperty(propertyName)) {
        throw BinderException(
            tableSchema->tableName + " table doesn't have property " + propertyName + ".");
    }
}

static void validatePropertyNotExist(TableSchema* tableSchema, const std::string& propertyName) {
    if (tableSchema->containProperty(propertyName)) {
        throw BinderException(
            tableSchema->tableName + " table already has property " + propertyName + ".");
    }
}

static void validatePropertyDDLOnTable(TableSchema* tableSchema, const std::string& ddlOperation) {
    switch (tableSchema->tableType) {
    case TableType::REL_GROUP:
    case TableType::RDF: {
        throw BinderException(
            stringFormat("Cannot {} property on table {} with type {}.", ddlOperation,
                tableSchema->tableName, TableTypeUtils::toString(tableSchema->tableType)));
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
    auto dataType = bindDataType(extraInfo->dataType);
    auto propertyName = extraInfo->propertyName;
    validateTableExist(tableName);
    auto tableID = catalog.getTableID(clientContext->getTx(), tableName);
    auto tableSchema = catalog.getTableSchema(clientContext->getTx(), tableID);
    validatePropertyDDLOnTable(tableSchema, "add");
    validatePropertyNotExist(tableSchema, propertyName);
    if (dataType->getLogicalTypeID() == LogicalTypeID::SERIAL) {
        throw BinderException("Serial property in node table must be the primary key.");
    }
    auto defaultVal = ExpressionBinder::implicitCastIfNecessary(
        expressionBinder.bindExpression(*extraInfo->defaultValue), *dataType);
    auto boundExtraInfo =
        std::make_unique<BoundExtraAddPropertyInfo>(propertyName, *dataType, std::move(defaultVal));
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
    auto tableID = catalog.getTableID(clientContext->getTx(), tableName);
    auto tableSchema = catalog.getTableSchema(clientContext->getTx(), tableID);
    validatePropertyDDLOnTable(tableSchema, "drop");
    validatePropertyExist(tableSchema, propertyName);
    auto propertyID = tableSchema->getPropertyID(propertyName);
    if (tableSchema->getTableType() == TableType::NODE &&
        ku_dynamic_cast<TableSchema*, NodeTableSchema*>(tableSchema)->getPrimaryKeyPropertyID() ==
            propertyID) {
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
    auto tableID = catalog.getTableID(clientContext->getTx(), tableName);
    auto tableSchema = catalog.getTableSchema(clientContext->getTx(), tableID);
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
