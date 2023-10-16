#include "binder/binder.h"
#include "binder/ddl/bound_alter.h"
#include "binder/ddl/bound_create_table.h"
#include "binder/ddl/bound_drop_table.h"
#include "catalog/node_table_schema.h"
#include "catalog/rel_table_group_schema.h"
#include "catalog/rel_table_schema.h"
#include "common/exception/binder.h"
#include "common/string_utils.h"
#include "parser/ddl/alter.h"
#include "parser/ddl/create_table.h"
#include "parser/ddl/drop.h"

using namespace kuzu::common;
using namespace kuzu::parser;
using namespace kuzu::catalog;

namespace kuzu {
namespace binder {

std::vector<std::unique_ptr<Property>> Binder::bindProperties(
    std::vector<std::pair<std::string, std::string>> propertyNameDataTypes) {
    std::vector<std::unique_ptr<Property>> boundPropertyNameDataTypes;
    std::unordered_set<std::string> boundPropertyNames;
    boundPropertyNames.reserve(propertyNameDataTypes.size());
    boundPropertyNameDataTypes.reserve(propertyNameDataTypes.size());
    for (auto& propertyNameDataType : propertyNameDataTypes) {
        if (boundPropertyNames.contains(propertyNameDataType.first)) {
            throw BinderException(StringUtils::string_format(
                "Duplicated column name: {}, column name must be unique.",
                propertyNameDataType.first));
        } else if (TableSchema::isReservedPropertyName(propertyNameDataType.first)) {
            throw BinderException(
                StringUtils::string_format("PropertyName: {} is an internal reserved propertyName.",
                    propertyNameDataType.first));
        }
        boundPropertyNameDataTypes.push_back(std::make_unique<Property>(
            propertyNameDataType.first, bindDataType(propertyNameDataType.second)));
        boundPropertyNames.emplace(propertyNameDataType.first);
    }
    return boundPropertyNameDataTypes;
}

static uint32_t bindPrimaryKey(const std::string& pkColName,
    std::vector<std::pair<std::string, std::string>> propertyNameDataTypes) {
    uint32_t primaryKeyIdx = UINT32_MAX;
    for (auto i = 0u; i < propertyNameDataTypes.size(); i++) {
        if (propertyNameDataTypes[i].first == pkColName) {
            primaryKeyIdx = i;
        }
    }
    if (primaryKeyIdx == UINT32_MAX) {
        throw BinderException(
            "Primary key " + pkColName + " does not match any of the predefined node properties.");
    }
    auto primaryKey = propertyNameDataTypes[primaryKeyIdx];
    StringUtils::toUpper(primaryKey.second);
    // We only support INT64, STRING and SERIAL column as the primary key.
    switch (LogicalTypeUtils::dataTypeFromString(primaryKey.second).getLogicalTypeID()) {
    case LogicalTypeID::INT64:
    case LogicalTypeID::STRING:
    case LogicalTypeID::SERIAL:
        break;
    default:
        throw BinderException(
            "Invalid primary key type: " + primaryKey.second + ". Expected STRING or INT64.");
    }
    return primaryKeyIdx;
}

std::unique_ptr<BoundCreateTableInfo> Binder::bindCreateTableInfo(
    const parser::CreateTableInfo* info) {
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
    default:                                                      // LCOV_EXCL_START
        throw NotImplementedException("Binder::bindCreateTable"); // LCOV_EXCL_STOP
    }
}

std::unique_ptr<BoundCreateTableInfo> Binder::bindCreateNodeTableInfo(const CreateTableInfo* info) {
    auto boundProperties = bindProperties(info->propertyNameDataTypes);
    auto extraInfo = (ExtraCreateNodeTableInfo*)info->extraInfo.get();
    auto primaryKeyIdx = bindPrimaryKey(extraInfo->pKName, info->propertyNameDataTypes);
    for (auto i = 0u; i < boundProperties.size(); ++i) {
        if (boundProperties[i]->getDataType()->getLogicalTypeID() == LogicalTypeID::SERIAL &&
            primaryKeyIdx != i) {
            throw BinderException("Serial property in node table must be the primary key.");
        }
    }
    auto boundExtraInfo =
        std::make_unique<BoundExtraCreateNodeTableInfo>(primaryKeyIdx, std::move(boundProperties));
    return std::make_unique<BoundCreateTableInfo>(
        TableType::NODE, info->tableName, std::move(boundExtraInfo));
}

std::unique_ptr<BoundCreateTableInfo> Binder::bindCreateRelTableInfo(const CreateTableInfo* info) {
    auto boundProperties = bindProperties(info->propertyNameDataTypes);
    for (auto& boundProperty : boundProperties) {
        if (boundProperty->getDataType()->getLogicalTypeID() == LogicalTypeID::SERIAL ||
            boundProperty->getDataType()->getLogicalTypeID() == LogicalTypeID::UNION ||
            boundProperty->getDataType()->getLogicalTypeID() == LogicalTypeID::STRUCT ||
            boundProperty->getDataType()->getLogicalTypeID() == LogicalTypeID::MAP) {
            throw BinderException(
                StringUtils::string_format("{} property is not supported in rel table.",
                    LogicalTypeUtils::dataTypeToString(
                        boundProperty->getDataType()->getLogicalTypeID())));
        }
    }
    auto extraInfo = (ExtraCreateRelTableInfo*)info->extraInfo.get();
    auto relMultiplicity = getRelMultiplicityFromString(extraInfo->relMultiplicity);
    auto srcTableID = bindTableID(extraInfo->srcTableName);
    validateTableType(srcTableID, TableType::NODE);
    auto dstTableID = bindTableID(extraInfo->dstTableName);
    validateTableType(dstTableID, TableType::NODE);
    auto boundExtraInfo = std::make_unique<BoundExtraCreateRelTableInfo>(
        relMultiplicity, srcTableID, dstTableID, std::move(boundProperties));
    return std::make_unique<BoundCreateTableInfo>(
        TableType::REL, info->tableName, std::move(boundExtraInfo));
}

std::unique_ptr<BoundCreateTableInfo> Binder::bindCreateRelTableGroupInfo(
    const CreateTableInfo* info) {
    auto relGroupName = info->tableName;
    auto extraInfo = (ExtraCreateRelTableGroupInfo*)info->extraInfo.get();
    auto relMultiplicity = extraInfo->relMultiplicity;
    std::vector<std::unique_ptr<BoundCreateTableInfo>> boundCreateRelTableInfos;
    for (auto& [srcTableName, dstTableName] : extraInfo->srcDstTablePairs) {
        auto relTableName = relGroupName + "_" + srcTableName + "_" + dstTableName;
        auto relExtraInfo =
            std::make_unique<ExtraCreateRelTableInfo>(relMultiplicity, srcTableName, dstTableName);
        auto relCreateInfo = std::make_unique<CreateTableInfo>(
            TableType::REL, relTableName, info->propertyNameDataTypes, std::move(relExtraInfo));
        boundCreateRelTableInfos.push_back(bindCreateRelTableInfo(relCreateInfo.get()));
    }
    auto boundExtraInfo =
        std::make_unique<BoundExtraCreateRelTableGroupInfo>(std::move(boundCreateRelTableInfos));
    return std::make_unique<BoundCreateTableInfo>(
        TableType::REL_GROUP, info->tableName, std::move(boundExtraInfo));
}

static inline std::string getRdfNodeTableName(const std::string& rdfName) {
    return rdfName + common::RDFKeyword::NODE_TABLE_SUFFIX;
}

static inline std::string getRdfRelTableName(const std::string& rdfName) {
    return rdfName + common::RDFKeyword::REL_TABLE_SUFFIX;
}

std::unique_ptr<BoundCreateTableInfo> Binder::bindCreateRdfGraphInfo(const CreateTableInfo* info) {
    auto rdfGraphName = info->tableName;
    auto stringType = std::make_unique<LogicalType>(LogicalTypeID::STRING);
    // RDF node (resource) table
    auto nodeTableName = getRdfNodeTableName(rdfGraphName);
    std::vector<std::unique_ptr<Property>> nodeProperties;
    nodeProperties.push_back(
        std::make_unique<Property>(common::RDFKeyword::IRI, stringType->copy()));
    auto boundNodeExtraInfo = std::make_unique<BoundExtraCreateNodeTableInfo>(
        0 /* primaryKeyIdx */, std::move(nodeProperties));
    auto boundNodeCreateInfo = std::make_unique<BoundCreateTableInfo>(
        TableType::NODE, nodeTableName, std::move(boundNodeExtraInfo));
    // RDF rel (triples) table
    auto relTableName = getRdfRelTableName(rdfGraphName);
    std::vector<std::unique_ptr<Property>> relProperties;
    relProperties.push_back(std::make_unique<Property>(
        common::RDFKeyword::PREDICT_ID, std::make_unique<LogicalType>(LogicalTypeID::INTERNAL_ID)));
    auto boundRelExtraInfo = std::make_unique<BoundExtraCreateRelTableInfo>(
        RelMultiplicity::MANY_MANY, INVALID_TABLE_ID, INVALID_TABLE_ID, std::move(relProperties));
    auto boundRelCreateInfo = std::make_unique<BoundCreateTableInfo>(
        TableType::REL, relTableName, std::move(boundRelExtraInfo));
    auto boundExtraInfo = std::make_unique<BoundExtraCreateRdfGraphInfo>(
        std::move(boundNodeCreateInfo), std::move(boundRelCreateInfo));
    return std::make_unique<BoundCreateTableInfo>(
        TableType::RDF, rdfGraphName, std::move(boundExtraInfo));
}

std::unique_ptr<BoundStatement> Binder::bindCreateTable(const parser::Statement& statement) {
    auto& createTable = reinterpret_cast<const CreateTable&>(statement);
    auto tableName = createTable.getTableName();
    if (catalog.getReadOnlyVersion()->containsTable(tableName)) {
        throw BinderException(tableName + " already exists in catalog.");
    }
    auto boundCreateInfo = bindCreateTableInfo(createTable.getInfo());
    return std::make_unique<BoundCreateTable>(std::move(boundCreateInfo));
}

std::unique_ptr<BoundStatement> Binder::bindDropTable(const Statement& statement) {
    auto& dropTable = reinterpret_cast<const Drop&>(statement);
    auto tableName = dropTable.getTableName();
    validateTableExist(tableName);
    auto catalogContent = catalog.getReadOnlyVersion();
    auto tableID = catalogContent->getTableID(tableName);
    auto tableSchema = catalogContent->getTableSchema(tableID);
    switch (tableSchema->tableType) {
    case TableType::NODE: {
        for (auto& schema : catalogContent->getRelTableSchemas()) {
            auto relTableSchema = reinterpret_cast<RelTableSchema*>(schema);
            if (relTableSchema->isSrcOrDstTable(tableID)) {
                throw BinderException(StringUtils::string_format(
                    "Cannot delete node table {} referenced by rel table {}.",
                    tableSchema->tableName, relTableSchema->tableName));
            }
        }
    } break;
    case TableType::REL: {
        for (auto& schema : catalogContent->getRelTableGroupSchemas()) {
            auto relTableGroupSchema = reinterpret_cast<RelTableGroupSchema*>(schema);
            for (auto& relTableID : relTableGroupSchema->getRelTableIDs()) {
                if (relTableID == tableSchema->getTableID()) {
                    throw BinderException(StringUtils::string_format(
                        "Cannot delete rel table {} referenced by rel group {}.",
                        tableSchema->tableName, relTableGroupSchema->tableName));
                }
            }
        }
    }
    default:
        break;
    }
    return make_unique<BoundDropTable>(tableID, tableName);
}

std::unique_ptr<BoundStatement> Binder::bindAlter(const parser::Statement& statement) {
    auto& alter = reinterpret_cast<const Alter&>(statement);
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
    default:
        throw NotImplementedException("Binder::bindAlter");
    }
}

std::unique_ptr<BoundStatement> Binder::bindRenameTable(const Statement& statement) {
    auto& alter = reinterpret_cast<const Alter&>(statement);
    auto info = alter.getInfo();
    auto extraInfo = reinterpret_cast<ExtraRenameTableInfo*>(info->extraInfo.get());
    auto tableName = info->tableName;
    auto newName = extraInfo->newName;
    auto catalogContent = catalog.getReadOnlyVersion();
    validateTableExist(tableName);
    if (catalogContent->containsTable(newName)) {
        throw BinderException("Table: " + newName + " already exists.");
    }
    auto tableID = catalogContent->getTableID(tableName);
    auto boundExtraInfo = std::make_unique<BoundExtraRenameTableInfo>(newName);
    auto boundInfo = std::make_unique<BoundAlterInfo>(
        AlterType::RENAME_TABLE, tableName, tableID, std::move(boundExtraInfo));
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
            StringUtils::string_format("Cannot {} property on table {} with type {}.", ddlOperation,
                tableSchema->tableName, TableTypeUtils::toString(tableSchema->tableType)));
    }
    default:
        return;
    }
}

std::unique_ptr<BoundStatement> Binder::bindAddProperty(const Statement& statement) {
    auto& alter = reinterpret_cast<const Alter&>(statement);
    auto info = alter.getInfo();
    auto extraInfo = reinterpret_cast<ExtraAddPropertyInfo*>(info->extraInfo.get());
    auto tableName = info->tableName;
    auto dataType = bindDataType(extraInfo->dataType);
    auto propertyName = extraInfo->propertyName;
    validateTableExist(tableName);
    auto catalogContent = catalog.getReadOnlyVersion();
    auto tableID = catalogContent->getTableID(tableName);
    auto tableSchema = catalogContent->getTableSchema(tableID);
    validatePropertyDDLOnTable(tableSchema, "add");
    validatePropertyNotExist(tableSchema, propertyName);
    if (dataType->getLogicalTypeID() == LogicalTypeID::SERIAL) {
        throw BinderException("Serial property in node table must be the primary key.");
    }
    auto defaultVal = ExpressionBinder::implicitCastIfNecessary(
        expressionBinder.bindExpression(*extraInfo->defaultValue), *dataType);
    auto boundExtraInfo = std::make_unique<BoundExtraAddPropertyInfo>(
        propertyName, dataType->copy(), std::move(defaultVal));
    auto boundInfo = std::make_unique<BoundAlterInfo>(
        AlterType::ADD_PROPERTY, tableName, tableID, std::move(boundExtraInfo));
    return std::make_unique<BoundAlter>(std::move(boundInfo));
}

std::unique_ptr<BoundStatement> Binder::bindDropProperty(const Statement& statement) {
    auto& alter = reinterpret_cast<const Alter&>(statement);
    auto info = alter.getInfo();
    auto extraInfo = reinterpret_cast<ExtraDropPropertyInfo*>(info->extraInfo.get());
    auto tableName = info->tableName;
    auto propertyName = extraInfo->propertyName;
    validateTableExist(tableName);
    auto catalogContent = catalog.getReadOnlyVersion();
    auto tableID = catalogContent->getTableID(tableName);
    auto tableSchema = catalogContent->getTableSchema(tableID);
    validatePropertyDDLOnTable(tableSchema, "drop");
    validatePropertyExist(tableSchema, propertyName);
    auto propertyID = tableSchema->getPropertyID(propertyName);
    if (tableSchema->getTableType() == TableType::NODE &&
        reinterpret_cast<NodeTableSchema*>(tableSchema)->getPrimaryKeyPropertyID() == propertyID) {
        throw BinderException("Cannot drop primary key of a node table.");
    }
    auto boundExtraInfo = std::make_unique<BoundExtraDropPropertyInfo>(propertyID);
    auto boundInfo = std::make_unique<BoundAlterInfo>(
        AlterType::DROP_PROPERTY, tableName, tableID, std::move(boundExtraInfo));
    return make_unique<BoundAlter>(std::move(boundInfo));
}

std::unique_ptr<BoundStatement> Binder::bindRenameProperty(const Statement& statement) {
    auto& alter = reinterpret_cast<const Alter&>(statement);
    auto info = alter.getInfo();
    auto extraInfo = reinterpret_cast<ExtraRenamePropertyInfo*>(info->extraInfo.get());
    auto tableName = info->tableName;
    auto propertyName = extraInfo->propertyName;
    auto newName = extraInfo->newName;
    validateTableExist(tableName);
    auto catalogContent = catalog.getReadOnlyVersion();
    auto tableID = catalogContent->getTableID(tableName);
    auto tableSchema = catalogContent->getTableSchema(tableID);
    validatePropertyDDLOnTable(tableSchema, "rename");
    validatePropertyExist(tableSchema, propertyName);
    auto propertyID = tableSchema->getPropertyID(propertyName);
    validatePropertyNotExist(tableSchema, newName);
    auto boundExtraInfo = std::make_unique<BoundExtraRenamePropertyInfo>(propertyID, newName);
    auto boundInfo = std::make_unique<BoundAlterInfo>(
        AlterType::RENAME_PROPERTY, tableName, tableID, std::move(boundExtraInfo));
    return std::make_unique<BoundAlter>(std::move(boundInfo));
}

} // namespace binder
} // namespace kuzu
