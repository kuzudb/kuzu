#include "binder/binder.h"
#include "binder/ddl/bound_alter.h"
#include "binder/ddl/bound_create_sequence.h"
#include "binder/ddl/bound_create_table.h"
#include "binder/ddl/bound_create_type.h"
#include "binder/ddl/bound_drop_sequence.h"
#include "binder/ddl/bound_drop_table.h"
#include "binder/expression/expression_util.h"
#include "catalog/catalog.h"
#include "catalog/catalog_entry/node_table_catalog_entry.h"
#include "catalog/catalog_entry/rdf_graph_catalog_entry.h"
#include "catalog/catalog_entry/rel_group_catalog_entry.h"
#include "catalog/catalog_entry/rel_table_catalog_entry.h"
#include "common/exception/binder.h"
#include "common/exception/message.h"
#include "common/string_format.h"
#include "common/types/types.h"
#include "function/cast/functions/cast_from_string_functions.h"
#include "function/sequence/sequence_functions.h"
#include "parser/ddl/alter.h"
#include "parser/ddl/create_sequence.h"
#include "parser/ddl/create_table.h"
#include "parser/ddl/create_table_info.h"
#include "parser/ddl/create_type.h"
#include "parser/ddl/drop.h"
#include "parser/expression/parsed_function_expression.h"
#include "parser/expression/parsed_literal_expression.h"

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

static void validateSerialNoDefault(const Expression& expr) {
    if (!ExpressionUtil::isNullLiteral(expr)) {
        throw BinderException("No DEFAULT value should be set for SERIAL columns");
    }
}

static std::unique_ptr<parser::ParsedFunctionExpression> createSerialDefaultExpr(std::string name) {
    auto param = std::make_unique<parser::ParsedLiteralExpression>(Value(name), "");
    return std::make_unique<parser::ParsedFunctionExpression>(function::NextValFunction::name,
        std::move(param), "");
}

std::vector<PropertyInfo> Binder::bindPropertyInfo(
    const std::vector<PropertyDefinitionDDL>& propertyDefinitions, const std::string& tableName) {
    std::vector<PropertyInfo> propertyInfos;
    propertyInfos.reserve(propertyDefinitions.size());
    for (auto& propertyDef : propertyDefinitions) {
        auto type = clientContext->getCatalog()->getType(clientContext->getTx(), propertyDef.type);
        auto expr = propertyDef.expr->copy();
        // This will check the type correctness of the default value expression
        auto boundExpr = expressionBinder.implicitCastIfNecessary(
            expressionBinder.bindExpression(*propertyDef.expr), type);
        if (type.getLogicalTypeID() == LogicalTypeID::SERIAL) {
            validateSerialNoDefault(*boundExpr);
            expr = createSerialDefaultExpr(Catalog::genSerialName(tableName, propertyDef.name));
        }
        propertyInfos.emplace_back(propertyDef.name, std::move(type), std::move(expr));
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
    auto propertyInfos = bindPropertyInfo(info->propertyDefinitions, info->tableName);
    auto& extraInfo = info->extraInfo->constCast<ExtraCreateNodeTableInfo>();
    auto primaryKeyIdx = bindPrimaryKey(extraInfo.pKName, propertyInfos);
    auto boundExtraInfo =
        std::make_unique<BoundExtraCreateNodeTableInfo>(primaryKeyIdx, std::move(propertyInfos));
    return BoundCreateTableInfo(TableType::NODE, info->tableName, info->onConflict,
        std::move(boundExtraInfo));
}

BoundCreateTableInfo Binder::bindCreateRelTableInfo(const CreateTableInfo* info) {
    std::vector<PropertyInfo> propertyInfos;
    propertyInfos.emplace_back(InternalKeyword::ID, *LogicalType::INTERNAL_ID());
    for (auto& propertyInfo : bindPropertyInfo(info->propertyDefinitions, info->tableName)) {
        propertyInfos.push_back(propertyInfo.copy());
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
    return BoundCreateTableInfo(TableType::REL, info->tableName, info->onConflict,
        std::move(boundExtraInfo));
}

BoundCreateTableInfo Binder::bindCreateRelTableGroupInfo(const CreateTableInfo* info) {
    auto relGroupName = info->tableName;
    auto extraInfo = (ExtraCreateRelTableGroupInfo*)info->extraInfo.get();
    auto relMultiplicity = extraInfo->relMultiplicity;
    std::vector<BoundCreateTableInfo> boundCreateRelTableInfos;
    auto relCreateInfo = std::make_unique<CreateTableInfo>(TableType::REL, "", info->onConflict);
    for (auto& propertyDef : info->propertyDefinitions) {
        relCreateInfo->propertyDefinitions.emplace_back(propertyDef.name, propertyDef.type,
            propertyDef.expr->copy());
    }
    for (auto& [srcTableName, dstTableName] : extraInfo->srcDstTablePairs) {
        relCreateInfo->tableName = std::string(relGroupName)
                                       .append("_")
                                       .append(srcTableName)
                                       .append("_")
                                       .append(dstTableName);
        relCreateInfo->extraInfo =
            std::make_unique<ExtraCreateRelTableInfo>(relMultiplicity, srcTableName, dstTableName);
        boundCreateRelTableInfos.push_back(bindCreateRelTableInfo(relCreateInfo.get()));
    }
    auto boundExtraInfo =
        std::make_unique<BoundExtraCreateRelTableGroupInfo>(std::move(boundCreateRelTableInfos));
    return BoundCreateTableInfo(TableType::REL_GROUP, info->tableName, info->onConflict,
        std::move(boundExtraInfo));
}

std::unique_ptr<BoundStatement> Binder::bindCreateTable(const Statement& statement) {
    auto createTable = statement.constPtrCast<CreateTable>();
    auto tableName = createTable->getInfo()->tableName;
    switch (createTable->getInfo()->onConflict) {
    case common::ConflictAction::ON_CONFLICT_THROW: {
        if (clientContext->getCatalog()->containsTable(clientContext->getTx(), tableName)) {
            throw BinderException(tableName + " already exists in catalog.");
        }
    } break;
    default:
        break;
    }
    auto boundCreateInfo = bindCreateTableInfo(createTable->getInfo());
    return std::make_unique<BoundCreateTable>(std::move(boundCreateInfo));
}

std::unique_ptr<BoundStatement> Binder::bindCreateType(const Statement& statement) {
    auto createType = statement.constPtrCast<CreateType>();
    auto name = createType->getName();
    auto type = LogicalType::fromString(createType->getDataType());
    if (clientContext->getCatalog()->containsType(clientContext->getTx(), name)) {
        throw BinderException{common::stringFormat("Duplicated type name: {}.", name)};
    }
    return std::make_unique<BoundCreateType>(std::move(name), std::move(type));
}

std::unique_ptr<BoundStatement> Binder::bindCreateSequence(const Statement& statement) {
    auto& createSequence = statement.constCast<CreateSequence>();
    auto info = createSequence.getInfo();
    auto sequenceName = info.sequenceName;
    int64_t startWith;
    int64_t increment;
    int64_t minValue;
    int64_t maxValue;
    ku_string_t literal;
    if (clientContext->getCatalog()->containsSequence(clientContext->getTx(), sequenceName)) {
        throw BinderException(sequenceName + " already exists in catalog.");
    }
    literal = ku_string_t{info.increment.c_str(), info.increment.length()};
    if (!function::CastString::tryCast(literal, increment)) {
        throw BinderException("Out of bounds: SEQUENCE accepts integers within INT64.");
    }
    if (increment == 0) {
        throw BinderException("INCREMENT must be non-zero.");
    }

    if (info.minValue == "") {
        minValue = increment > 0 ? 1 : std::numeric_limits<int64_t>::min();
    } else {
        literal = ku_string_t{info.minValue.c_str(), info.minValue.length()};
        if (!function::CastString::tryCast(literal, minValue)) {
            throw BinderException("Out of bounds: SEQUENCE accepts integers within INT64.");
        }
    }
    if (info.maxValue == "") {
        maxValue = increment > 0 ? std::numeric_limits<int64_t>::max() : -1;
    } else {
        literal = ku_string_t{info.maxValue.c_str(), info.maxValue.length()};
        if (!function::CastString::tryCast(literal, maxValue)) {
            throw BinderException("Out of bounds: SEQUENCE accepts integers within INT64.");
        }
    }
    if (info.startWith == "") {
        startWith = increment > 0 ? minValue : maxValue;
    } else {
        literal = ku_string_t{info.startWith.c_str(), info.startWith.length()};
        if (!function::CastString::tryCast(literal, startWith)) {
            throw BinderException("Out of bounds: SEQUENCE accepts integers within INT64.");
        }
    }

    if (maxValue < minValue) {
        throw BinderException("SEQUENCE MAXVALUE should be greater than or equal to MINVALUE.");
    }
    if (startWith < minValue || startWith > maxValue) {
        throw BinderException("SEQUENCE START value should be between MINVALUE and MAXVALUE.");
    }

    auto boundInfo =
        BoundCreateSequenceInfo(sequenceName, startWith, increment, minValue, maxValue, info.cycle);
    return std::make_unique<BoundCreateSequence>(std::move(boundInfo));
}

std::unique_ptr<BoundStatement> Binder::bindDropTable(const Statement& statement) {
    auto& dropTable = statement.constCast<Drop>();
    auto tableName = dropTable.getName();
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
        auto& rdfGraphEntry = tableEntry->constCast<RDFGraphCatalogEntry>();
        // Check resource table is not referenced by rel table other than its triple table.
        for (auto& relTableEntry : catalog->getRelTableEntries(clientContext->getTx())) {
            if (relTableEntry->getTableID() == rdfGraphEntry.getResourceTripleTableID() ||
                relTableEntry->getTableID() == rdfGraphEntry.getLiteralTripleTableID()) {
                continue;
            }
            if (relTableEntry->isParent(rdfGraphEntry.getResourceTableID())) {
                throw BinderException(stringFormat("Cannot delete rdfGraph {} because its resource "
                                                   "table is referenced by relationship table {}.",
                    tableEntry->getName(), relTableEntry->getName()));
            }
        }
        // Check literal table is not referenced by rel table other than its triple table.
        for (auto& relTableEntry : catalog->getRelTableEntries(clientContext->getTx())) {
            if (relTableEntry->getTableID() == rdfGraphEntry.getLiteralTripleTableID()) {
                continue;
            }
            if (relTableEntry->isParent(rdfGraphEntry.getLiteralTableID())) {
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

std::unique_ptr<BoundStatement> Binder::bindDropSequence(const Statement& statement) {
    auto& dropSequence = statement.constCast<Drop>();
    auto sequenceName = dropSequence.getName();
    if (!clientContext->getCatalog()->containsSequence(clientContext->getTx(), sequenceName)) {
        throw BinderException("Sequence " + sequenceName + " does not exist.");
    }
    auto catalog = clientContext->getCatalog();
    auto sequenceID = catalog->getSequenceID(clientContext->getTx(), sequenceName);
    // TODO: Later check if sequence used/referenced by table
    return make_unique<BoundDropSequence>(sequenceID, sequenceName);
}

std::unique_ptr<BoundStatement> Binder::bindAlter(const Statement& statement) {
    auto& alter = statement.constCast<Alter>();
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
    auto& alter = statement.constCast<Alter>();
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
    auto& alter = statement.constCast<Alter>();
    auto info = alter.getInfo();
    auto extraInfo = ku_dynamic_cast<ExtraAlterInfo*, ExtraAddPropertyInfo*>(info->extraInfo.get());
    auto tableName = info->tableName;
    auto dataType =
        clientContext->getCatalog()->getType(clientContext->getTx(), extraInfo->dataType);
    if (extraInfo->defaultValue == nullptr) {
        extraInfo->defaultValue =
            std::make_unique<ParsedLiteralExpression>(Value::createNullValue(dataType), "NULL");
    }
    auto propertyName = extraInfo->propertyName;
    validateTableExist(tableName);
    auto catalog = clientContext->getCatalog();
    auto tableID = catalog->getTableID(clientContext->getTx(), tableName);
    auto tableSchema = catalog->getTableCatalogEntry(clientContext->getTx(), tableID);
    validatePropertyDDLOnTable(tableSchema, "add");
    validatePropertyNotExist(tableSchema, propertyName);
    auto defaultValue = std::move(extraInfo->defaultValue);
    auto boundDefault = expressionBinder.implicitCastIfNecessary(
        expressionBinder.bindExpression(*defaultValue), dataType);
    if (dataType.getLogicalTypeID() == LogicalTypeID::SERIAL) {
        validateSerialNoDefault(*boundDefault);
    }
    auto boundExtraInfo = std::make_unique<BoundExtraAddPropertyInfo>(propertyName, dataType,
        std::move(defaultValue), std::move(boundDefault));
    auto boundInfo =
        BoundAlterInfo(AlterType::ADD_PROPERTY, tableName, tableID, std::move(boundExtraInfo));
    return std::make_unique<BoundAlter>(std::move(boundInfo));
}

std::unique_ptr<BoundStatement> Binder::bindDropProperty(const Statement& statement) {
    auto& alter = statement.constCast<Alter>();
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
    auto& alter = statement.constCast<Alter>();
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
