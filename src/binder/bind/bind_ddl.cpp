#include "binder/binder.h"
#include "binder/ddl/bound_add_property.h"
#include "binder/ddl/bound_create_table.h"
#include "binder/ddl/bound_drop_property.h"
#include "binder/ddl/bound_drop_table.h"
#include "binder/ddl/bound_rename_property.h"
#include "binder/ddl/bound_rename_table.h"
#include "catalog/node_table_schema.h"
#include "catalog/rel_table_schema.h"
#include "common/exception/binder.h"
#include "common/string_utils.h"
#include "parser/ddl/add_property.h"
#include "parser/ddl/create_table.h"
#include "parser/ddl/drop_property.h"
#include "parser/ddl/drop_table.h"
#include "parser/ddl/rename_property.h"
#include "parser/ddl/rename_table.h"

using namespace kuzu::common;
using namespace kuzu::parser;
using namespace kuzu::catalog;

namespace kuzu {
namespace binder {

static std::unique_ptr<LogicalType> bindDataType(const std::string& dataType) {
    auto boundType = LogicalTypeUtils::dataTypeFromString(dataType);
    if (boundType.getLogicalTypeID() == LogicalTypeID::FIXED_LIST) {
        auto validNumericTypes = LogicalTypeUtils::getNumericalLogicalTypeIDs();
        auto childType = FixedListType::getChildType(&boundType);
        auto numElementsInList = FixedListType::getNumElementsInList(&boundType);
        if (find(validNumericTypes.begin(), validNumericTypes.end(),
                childType->getLogicalTypeID()) == validNumericTypes.end()) {
            throw BinderException("The child type of a fixed list must be a numeric type. Given: " +
                                  LogicalTypeUtils::dataTypeToString(*childType) + ".");
        }
        if (numElementsInList == 0) {
            // Note: the parser already guarantees that the number of elements is a non-negative
            // number. However, we still need to check whether the number of elements is 0.
            throw BinderException(
                "The number of elements in a fixed list must be greater than 0. Given: " +
                std::to_string(numElementsInList) + ".");
        }
        auto numElementsPerPage = storage::PageUtils::getNumElementsInAPage(
            storage::StorageUtils::getDataTypeSize(boundType), true /* hasNull */);
        if (numElementsPerPage == 0) {
            throw BinderException(
                StringUtils::string_format("Cannot store a fixed list of size {} in a page.",
                    storage::StorageUtils::getDataTypeSize(boundType)));
        }
    }
    return std::make_unique<LogicalType>(boundType);
}

static std::vector<std::unique_ptr<Property>> bindProperties(
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
        if (boundProperty->getDataType()->getLogicalTypeID() == LogicalTypeID::SERIAL) {
            throw BinderException("Serial property is not supported in rel table.");
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
    return std::make_unique<BoundCreateTable>(tableName, std::move(boundCreateInfo));
}

std::unique_ptr<BoundStatement> Binder::bindDropTable(const Statement& statement) {
    auto& dropTable = (DropTable&)statement;
    auto tableName = dropTable.getTableName();
    validateNodeRelTableExist(tableName);
    auto catalogContent = catalog.getReadOnlyVersion();
    auto tableID = catalogContent->getTableID(tableName);
    if (catalogContent->containsNodeTable(tableName)) {
        validateNodeTableHasNoEdge(catalog, tableID);
    }
    return make_unique<BoundDropTable>(tableID, tableName);
}

std::unique_ptr<BoundStatement> Binder::bindRenameTable(const Statement& statement) {
    auto renameTable = (RenameTable&)statement;
    auto tableName = renameTable.getTableName();
    auto catalogContent = catalog.getReadOnlyVersion();
    validateNodeRelTableExist(tableName);
    if (catalogContent->containsTable(renameTable.getNewName())) {
        throw BinderException("Table: " + renameTable.getNewName() + " already exists.");
    }
    return make_unique<BoundRenameTable>(
        catalogContent->getTableID(tableName), tableName, renameTable.getNewName());
}

std::unique_ptr<BoundStatement> Binder::bindAddProperty(const Statement& statement) {
    auto& addProperty = (AddProperty&)statement;
    auto tableName = addProperty.getTableName();
    validateNodeRelTableExist(tableName);
    auto catalogContent = catalog.getReadOnlyVersion();
    auto tableID = catalogContent->getTableID(tableName);
    auto dataType = bindDataType(addProperty.getDataType());
    if (catalogContent->getTableSchema(tableID)->containProperty(addProperty.getPropertyName())) {
        throw BinderException("Property: " + addProperty.getPropertyName() + " already exists.");
    }
    if (dataType->getLogicalTypeID() == LogicalTypeID::SERIAL) {
        throw BinderException("Serial property in node table must be the primary key.");
    }
    auto defaultVal = ExpressionBinder::implicitCastIfNecessary(
        expressionBinder.bindExpression(*addProperty.getDefaultValue()), *dataType);
    return make_unique<BoundAddProperty>(
        tableID, addProperty.getPropertyName(), std::move(dataType), defaultVal, tableName);
}

std::unique_ptr<BoundStatement> Binder::bindDropProperty(const Statement& statement) {
    auto& dropProperty = (DropProperty&)statement;
    auto tableName = dropProperty.getTableName();
    validateNodeRelTableExist(tableName);
    auto catalogContent = catalog.getReadOnlyVersion();
    auto tableID = catalogContent->getTableID(tableName);
    auto tableSchema = catalogContent->getTableSchema(tableID);
    auto propertyID = bindPropertyName(tableSchema, dropProperty.getPropertyName());
    if (tableSchema->getTableType() == TableType::NODE &&
        reinterpret_cast<NodeTableSchema*>(tableSchema)->getPrimaryKeyPropertyID() == propertyID) {
        throw BinderException("Cannot drop primary key of a node table.");
    }
    return make_unique<BoundDropProperty>(tableID, propertyID, tableName);
}

std::unique_ptr<BoundStatement> Binder::bindRenameProperty(const Statement& statement) {
    auto& renameProperty = (RenameProperty&)statement;
    auto tableName = renameProperty.getTableName();
    validateNodeRelTableExist(tableName);
    auto catalogContent = catalog.getReadOnlyVersion();
    auto tableID = catalogContent->getTableID(tableName);
    auto tableSchema = catalogContent->getTableSchema(tableID);
    auto propertyID = bindPropertyName(tableSchema, renameProperty.getPropertyName());
    if (tableSchema->containProperty(renameProperty.getNewName())) {
        throw BinderException("Property " + renameProperty.getNewName() +
                              " already exists in table: " + tableName + ".");
    }
    return make_unique<BoundRenameProperty>(
        tableID, tableName, propertyID, renameProperty.getNewName());
}

property_id_t Binder::bindPropertyName(TableSchema* tableSchema, const std::string& propertyName) {
    for (auto& property : tableSchema->properties) {
        if (property->getName() == propertyName) {
            return property->getPropertyID();
        }
    }
    throw BinderException(
        tableSchema->tableName + " table doesn't have property: " + propertyName + ".");
}

} // namespace binder
} // namespace kuzu
