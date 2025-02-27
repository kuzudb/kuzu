#include "binder/binder.h"
#include "binder/ddl/bound_alter.h"
#include "binder/ddl/bound_create_sequence.h"
#include "binder/ddl/bound_create_table.h"
#include "binder/ddl/bound_create_type.h"
#include "binder/ddl/bound_drop.h"
#include "binder/expression_visitor.h"
#include "catalog/catalog.h"
#include "catalog/catalog_entry/index_catalog_entry.h"
#include "catalog/catalog_entry/rel_group_catalog_entry.h"
#include "catalog/catalog_entry/rel_table_catalog_entry.h"
#include "catalog/catalog_entry/sequence_catalog_entry.h"
#include "common/enums/extend_direction_util.h"
#include "common/exception/binder.h"
#include "common/exception/message.h"
#include "common/string_format.h"
#include "common/system_config.h"
#include "common/types/types.h"
#include "function/cast/functions/cast_from_string_functions.h"
#include "function/sequence/sequence_functions.h"
#include "main/client_context.h"
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

static void validatePropertyName(const std::vector<PropertyDefinition>& definitions) {
    case_insensitve_set_t nameSet;
    for (auto& definition : definitions) {
        if (nameSet.contains(definition.getName())) {
            throw BinderException(stringFormat(
                "Duplicated column name: {}, column name must be unique.", definition.getName()));
        }
        if (Binder::reservedInColumnName(definition.getName())) {
            throw BinderException(
                stringFormat("{} is a reserved property name.", definition.getName()));
        }
        nameSet.insert(definition.getName());
    }
}

std::vector<PropertyDefinition> Binder::bindPropertyDefinitions(
    const std::vector<ParsedPropertyDefinition>& parsedDefinitions, const std::string& tableName) {
    std::vector<PropertyDefinition> definitions;
    for (auto& def : parsedDefinitions) {
        auto type = LogicalType::convertFromString(def.getType(), clientContext);
        auto defaultExpr =
            resolvePropertyDefault(def.defaultExpr.get(), type, tableName, def.getName());
        auto columnDefinition = ColumnDefinition(def.getName(), std::move(type));
        definitions.emplace_back(std::move(columnDefinition), std::move(defaultExpr));
    }
    validatePropertyName(definitions);
    return definitions;
}

std::unique_ptr<parser::ParsedExpression> Binder::resolvePropertyDefault(
    ParsedExpression* parsedDefault, const common::LogicalType& type, const std::string& tableName,
    const std::string& propertyName) {
    if (parsedDefault == nullptr) { // No default provided.
        if (type.getLogicalTypeID() == LogicalTypeID::SERIAL) {
            auto serialName = SequenceCatalogEntry::getSerialName(tableName, propertyName);
            auto literalExpr = std::make_unique<ParsedLiteralExpression>(Value(serialName), "");
            return std::make_unique<ParsedFunctionExpression>(function::NextValFunction::name,
                std::move(literalExpr), "" /* rawName */);
        } else {
            return std::make_unique<ParsedLiteralExpression>(Value::createNullValue(type), "NULL");
        }
    } else {
        if (type.getLogicalTypeID() == LogicalTypeID::SERIAL) {
            throw BinderException("No DEFAULT value should be set for SERIAL columns");
        }
        return parsedDefault->copy();
    }
}

static void validatePrimaryKey(const std::string& pkColName,
    const std::vector<PropertyDefinition>& definitions) {
    uint32_t primaryKeyIdx = UINT32_MAX;
    for (auto i = 0u; i < definitions.size(); i++) {
        if (definitions[i].getName() == pkColName) {
            primaryKeyIdx = i;
        }
    }
    if (primaryKeyIdx == UINT32_MAX) {
        throw BinderException(
            "Primary key " + pkColName + " does not match any of the predefined node properties.");
    }
    const auto& pkType = definitions[primaryKeyIdx].getType();
    if (!pkType.isInternalType()) {
        throw BinderException(ExceptionMessage::invalidPKType(pkType.toString()));
    }
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
}

void Binder::validateNoIndexOnProperty(const std::string& tableName,
    const std::string& propertyName) const {
    auto transaction = clientContext->getTransaction();
    auto catalog = clientContext->getCatalog();
    if (catalog->containsRelGroup(transaction, tableName)) {
        // RelGroup does not have indexes.
        return;
    }
    auto tableEntry = catalog->getTableCatalogEntry(transaction, tableName);
    if (!tableEntry->containsProperty(propertyName)) {
        return;
    }
    auto propertyID = tableEntry->getPropertyID(propertyName);
    for (auto indexCatalogEntry : catalog->getIndexEntries(transaction)) {
        auto propertiesWithIndex = indexCatalogEntry->getPropertyIDs();
        if (indexCatalogEntry->getTableID() == tableEntry->getTableID() &&
            std::find(propertiesWithIndex.begin(), propertiesWithIndex.end(), propertyID) !=
                propertiesWithIndex.end()) {
            throw BinderException{stringFormat(
                "Cannot drop property {} in table {} because it is used in one or more indexes. "
                "Please remove the associated indexes before attempting to drop this property.",
                propertyName, tableName)};
        }
    }
}

BoundCreateTableInfo Binder::bindCreateTableInfo(const CreateTableInfo* info) {
    switch (info->type) {
    case CatalogEntryType::NODE_TABLE_ENTRY: {
        return bindCreateNodeTableInfo(info);
    }
    case CatalogEntryType::REL_TABLE_ENTRY: {
        return bindCreateRelTableInfo(info);
    }
    case CatalogEntryType::REL_GROUP_ENTRY: {
        return bindCreateRelTableGroupInfo(info);
    }
    default: {
        KU_UNREACHABLE;
    }
    }
}

BoundCreateTableInfo Binder::bindCreateNodeTableInfo(const CreateTableInfo* info) {
    auto propertyDefinitions = bindPropertyDefinitions(info->propertyDefinitions, info->tableName);
    auto& extraInfo = info->extraInfo->constCast<ExtraCreateNodeTableInfo>();
    validatePrimaryKey(extraInfo.pKName, propertyDefinitions);
    auto boundExtraInfo = std::make_unique<BoundExtraCreateNodeTableInfo>(extraInfo.pKName,
        std::move(propertyDefinitions));
    return BoundCreateTableInfo(CatalogEntryType::NODE_TABLE_ENTRY, info->tableName,
        info->onConflict, std::move(boundExtraInfo), clientContext->useInternalCatalogEntry());
}

void Binder::validateNodeTableType(const TableCatalogEntry* entry) {
    if (entry->getType() != CatalogEntryType::NODE_TABLE_ENTRY) {
        throw BinderException(stringFormat("{} is not of type NODE.", entry->getName()));
    }
}

void Binder::validateTableExistence(const main::ClientContext& context,
    const std::string& tableName) {
    if (!context.getCatalog()->containsTable(context.getTransaction(), tableName)) {
        throw BinderException{stringFormat("Table {} does not exist.", tableName)};
    }
}

void Binder::validateColumnExistence(const TableCatalogEntry* entry,
    const std::string& columnName) {
    if (!entry->containsProperty(columnName)) {
        throw BinderException{
            stringFormat("Column {} does not exist in table {}.", columnName, entry->getName())};
    }
}

static ExtendDirection getStorageDirection(const case_insensitive_map_t<Value>& options) {
    if (options.contains(TableOptionConstants::REL_STORAGE_DIRECTION_OPTION)) {
        return ExtendDirectionUtil::fromString(
            options.at(TableOptionConstants::REL_STORAGE_DIRECTION_OPTION).toString());
    }
    return DEFAULT_EXTEND_DIRECTION;
}

BoundCreateTableInfo Binder::bindCreateRelTableInfo(const CreateTableInfo* info) {
    auto& extraInfo = info->extraInfo->constCast<ExtraCreateRelTableInfo>();
    return bindCreateRelTableInfo(info, extraInfo.options);
}

BoundCreateTableInfo Binder::bindCreateRelTableInfo(const CreateTableInfo* info,
    const options_t& parsedOptions) {
    std::vector<PropertyDefinition> propertyDefinitions;
    propertyDefinitions.emplace_back(
        ColumnDefinition(InternalKeyword::ID, LogicalType::INTERNAL_ID()));
    for (auto& definition : bindPropertyDefinitions(info->propertyDefinitions, info->tableName)) {
        propertyDefinitions.push_back(definition.copy());
    }
    auto& extraInfo = info->extraInfo->constCast<ExtraCreateRelTableInfo>();
    auto srcMultiplicity = RelMultiplicityUtils::getFwd(extraInfo.relMultiplicity);
    auto dstMultiplicity = RelMultiplicityUtils::getBwd(extraInfo.relMultiplicity);
    auto srcEntry = bindNodeTableEntry(extraInfo.srcTableName);
    validateNodeTableType(srcEntry);
    auto dstEntry = bindNodeTableEntry(extraInfo.dstTableName);
    validateNodeTableType(dstEntry);
    auto boundOptions = bindParsingOptions(parsedOptions);
    auto storageDirection = getStorageDirection(boundOptions);
    auto boundExtraInfo = std::make_unique<BoundExtraCreateRelTableInfo>(srcMultiplicity,
        dstMultiplicity, storageDirection, srcEntry->getTableID(), dstEntry->getTableID(),
        std::move(propertyDefinitions));
    return BoundCreateTableInfo(CatalogEntryType::REL_TABLE_ENTRY, info->tableName,
        info->onConflict, std::move(boundExtraInfo), clientContext->useInternalCatalogEntry());
}

static void validateUniqueFromToPairs(
    const std::vector<std::pair<std::string, std::string>>& pairs) {
    std::unordered_set<std::string> set;
    for (auto [from, to] : pairs) {
        auto key = from + to;
        if (set.contains(key)) {
            throw BinderException(stringFormat("Found duplicate FROM-TO {}-{} pairs.", from, to));
        }
        set.insert(key);
    }
}

BoundCreateTableInfo Binder::bindCreateRelTableGroupInfo(const CreateTableInfo* info) {
    auto relGroupName = info->tableName;
    auto& extraInfo = info->extraInfo->constCast<ExtraCreateRelTableGroupInfo>();
    auto relMultiplicity = extraInfo.relMultiplicity;
    std::vector<BoundCreateTableInfo> boundCreateRelTableInfos;
    auto relCreateInfo =
        std::make_unique<CreateTableInfo>(CatalogEntryType::REL_TABLE_ENTRY, "", info->onConflict);
    relCreateInfo->propertyDefinitions = copyVector(info->propertyDefinitions);
    validateUniqueFromToPairs(extraInfo.srcDstTablePairs);
    for (auto& [srcTableName, dstTableName] : extraInfo.srcDstTablePairs) {
        relCreateInfo->tableName =
            RelGroupCatalogEntry::getChildTableName(relGroupName, srcTableName, dstTableName);
        relCreateInfo->extraInfo = std::make_unique<ExtraCreateRelTableInfo>(relMultiplicity,
            srcTableName, dstTableName, options_t{});
        auto boundInfo = bindCreateRelTableInfo(relCreateInfo.get(), extraInfo.options);
        boundInfo.hasParent = true;
        boundCreateRelTableInfos.push_back(std::move(boundInfo));
    }
    auto boundExtraInfo =
        std::make_unique<BoundExtraCreateRelTableGroupInfo>(std::move(boundCreateRelTableInfos));
    return BoundCreateTableInfo(CatalogEntryType::REL_GROUP_ENTRY, info->tableName,
        info->onConflict, std::move(boundExtraInfo), clientContext->useInternalCatalogEntry());
}

std::unique_ptr<BoundStatement> Binder::bindCreateTable(const Statement& statement) {
    auto createTable = statement.constPtrCast<CreateTable>();
    auto boundCreateInfo = bindCreateTableInfo(createTable->getInfo());
    return std::make_unique<BoundCreateTable>(std::move(boundCreateInfo));
}

std::unique_ptr<BoundStatement> Binder::bindCreateType(const Statement& statement) const {
    auto createType = statement.constPtrCast<CreateType>();
    auto name = createType->getName();
    LogicalType type = LogicalType::convertFromString(createType->getDataType(), clientContext);
    if (clientContext->getCatalog()->containsType(clientContext->getTransaction(), name)) {
        throw BinderException{stringFormat("Duplicated type name: {}.", name)};
    }
    return std::make_unique<BoundCreateType>(std::move(name), std::move(type));
}

std::unique_ptr<BoundStatement> Binder::bindCreateSequence(const Statement& statement) const {
    auto& createSequence = statement.constCast<CreateSequence>();
    auto info = createSequence.getInfo();
    auto sequenceName = info.sequenceName;
    int64_t startWith = 0;
    int64_t increment = 0;
    int64_t minValue = 0;
    int64_t maxValue = 0;
    switch (info.onConflict) {
    case ConflictAction::ON_CONFLICT_THROW: {
        if (clientContext->getCatalog()->containsSequence(clientContext->getTransaction(),
                sequenceName)) {
            throw BinderException(sequenceName + " already exists in catalog.");
        }
    } break;
    default:
        break;
    }
    auto literal = ku_string_t{info.increment.c_str(), info.increment.length()};
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

    auto boundInfo = BoundCreateSequenceInfo(sequenceName, startWith, increment, minValue, maxValue,
        info.cycle, info.onConflict, false /* isInternal */);
    return std::make_unique<BoundCreateSequence>(std::move(boundInfo));
}

std::unique_ptr<BoundStatement> Binder::bindDrop(const Statement& statement) {
    auto& drop = statement.constCast<Drop>();
    return std::make_unique<BoundDrop>(drop.getDropInfo());
}

std::unique_ptr<BoundStatement> Binder::bindAlter(const Statement& statement) {
    auto& alter = statement.constCast<Alter>();
    switch (alter.getInfo()->type) {
    case AlterType::RENAME: {
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
    case AlterType::COMMENT: {
        return bindCommentOn(statement);
    }
    default: {
        KU_UNREACHABLE;
    }
    }
}

std::unique_ptr<BoundStatement> Binder::bindRenameTable(const Statement& statement) const {
    auto& alter = statement.constCast<Alter>();
    auto info = alter.getInfo();
    auto extraInfo = ku_dynamic_cast<ExtraRenameTableInfo*>(info->extraInfo.get());
    auto tableName = info->tableName;
    auto newName = extraInfo->newName;
    auto boundExtraInfo = std::make_unique<BoundExtraRenameTableInfo>(newName);
    auto boundInfo =
        BoundAlterInfo(AlterType::RENAME, tableName, std::move(boundExtraInfo), info->onConflict);
    return std::make_unique<BoundAlter>(std::move(boundInfo));
}

std::unique_ptr<BoundStatement> Binder::bindAddProperty(const Statement& statement) {
    auto& alter = statement.constCast<Alter>();
    auto info = alter.getInfo();
    auto extraInfo = info->extraInfo->ptrCast<ExtraAddPropertyInfo>();
    auto tableName = info->tableName;
    auto propertyName = extraInfo->propertyName;
    auto type = LogicalType::convertFromString(extraInfo->dataType, clientContext);
    auto columnDefinition = ColumnDefinition(propertyName, type.copy());
    auto defaultExpr =
        resolvePropertyDefault(extraInfo->defaultValue.get(), type, tableName, propertyName);
    auto boundDefault = expressionBinder.bindExpression(*defaultExpr);
    boundDefault = expressionBinder.implicitCastIfNecessary(boundDefault, type);
    if (ConstantExpressionVisitor::needFold(*boundDefault)) {
        boundDefault = expressionBinder.foldExpression(boundDefault);
    }
    auto propertyDefinition =
        PropertyDefinition(std::move(columnDefinition), std::move(defaultExpr));
    auto boundExtraInfo = std::make_unique<BoundExtraAddPropertyInfo>(std::move(propertyDefinition),
        std::move(boundDefault));
    auto boundInfo = BoundAlterInfo(AlterType::ADD_PROPERTY, tableName, std::move(boundExtraInfo),
        info->onConflict);
    return std::make_unique<BoundAlter>(std::move(boundInfo));
}

std::unique_ptr<BoundStatement> Binder::bindDropProperty(const Statement& statement) const {
    auto& alter = statement.constCast<Alter>();
    auto info = alter.getInfo();
    auto extraInfo = info->extraInfo->constPtrCast<ExtraDropPropertyInfo>();
    auto tableName = info->tableName;
    auto propertyName = extraInfo->propertyName;
    validateNoIndexOnProperty(tableName, propertyName);
    auto boundExtraInfo = std::make_unique<BoundExtraDropPropertyInfo>(propertyName);
    auto boundInfo = BoundAlterInfo(AlterType::DROP_PROPERTY, tableName, std::move(boundExtraInfo),
        info->onConflict);
    return std::make_unique<BoundAlter>(std::move(boundInfo));
}

std::unique_ptr<BoundStatement> Binder::bindRenameProperty(const Statement& statement) const {
    auto& alter = statement.constCast<Alter>();
    auto info = alter.getInfo();
    auto extraInfo = info->extraInfo->constPtrCast<ExtraRenamePropertyInfo>();
    auto tableName = info->tableName;
    auto propertyName = extraInfo->propertyName;
    auto newName = extraInfo->newName;
    auto boundExtraInfo = std::make_unique<BoundExtraRenamePropertyInfo>(newName, propertyName);
    auto boundInfo = BoundAlterInfo(AlterType::RENAME_PROPERTY, tableName,
        std::move(boundExtraInfo), info->onConflict);
    return std::make_unique<BoundAlter>(std::move(boundInfo));
}

std::unique_ptr<BoundStatement> Binder::bindCommentOn(const Statement& statement) const {
    auto& alter = statement.constCast<Alter>();
    auto info = alter.getInfo();
    auto extraInfo = info->extraInfo->constPtrCast<ExtraCommentInfo>();
    auto tableName = info->tableName;
    auto comment = extraInfo->comment;
    auto boundExtraInfo = std::make_unique<BoundExtraCommentInfo>(comment);
    auto boundInfo =
        BoundAlterInfo(AlterType::COMMENT, tableName, std::move(boundExtraInfo), info->onConflict);
    return std::make_unique<BoundAlter>(std::move(boundInfo));
}

} // namespace binder
} // namespace kuzu
