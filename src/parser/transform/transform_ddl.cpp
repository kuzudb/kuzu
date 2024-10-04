#include "common/exception/parser.h"
#include "parser/ddl/alter.h"
#include "parser/ddl/create_sequence.h"
#include "parser/ddl/create_table.h"
#include "parser/ddl/create_type.h"
#include "parser/ddl/drop.h"
#include "parser/ddl/drop_info.h"
#include "parser/expression/parsed_literal_expression.h"
#include "parser/transformer.h"

using namespace kuzu::common;

namespace kuzu {
namespace parser {

std::unique_ptr<Statement> Transformer::transformAlterTable(
    CypherParser::KU_AlterTableContext& ctx) {
    if (ctx.kU_AlterOptions()->kU_AddProperty()) {
        return transformAddProperty(ctx);
    } else if (ctx.kU_AlterOptions()->kU_DropProperty()) {
        return transformDropProperty(ctx);
    } else if (ctx.kU_AlterOptions()->kU_RenameTable()) {
        return transformRenameTable(ctx);
    } else {
        return transformRenameProperty(ctx);
    }
}

std::string Transformer::getPKName(CypherParser::KU_CreateNodeTableContext& ctx) {
    auto pkCount = 0;
    std::string pkName;
    auto& propertyDefinitions = *ctx.kU_PropertyDefinitions();
    for (auto& definition : propertyDefinitions.kU_PropertyDefinition()) {
        if (definition->PRIMARY() && definition->KEY()) {
            pkCount++;
            pkName = transformPrimaryKey(*definition->kU_ColumnDefinition());
        }
    }
    if (ctx.kU_CreateNodeConstraint()) {
        // In the case where no pkName has been found, or the Node Constraint's name is different
        // than the pkName found, add the counter.
        if (pkCount == 0 || transformPrimaryKey(*ctx.kU_CreateNodeConstraint()) != pkName) {
            pkCount++;
        }
        pkName = transformPrimaryKey(*ctx.kU_CreateNodeConstraint());
    }
    if (pkCount == 0) {
        // Raise exception when no PRIMARY KEY is specified.
        throw ParserException("Can not find primary key.");
    } else if (pkCount > 1) {
        // Raise exception when multiple PRIMARY KEY are specified.
        throw ParserException("Found multiple primary keys.");
    }
    return pkName;
}

std::unique_ptr<Statement> Transformer::transformCreateNodeTable(
    CypherParser::KU_CreateNodeTableContext& ctx) {
    auto tableName = transformSchemaName(*ctx.oC_SchemaName());
    std::string pkName;
    pkName = getPKName(ctx);
    auto createTableInfo = CreateTableInfo(TableType::NODE, tableName,
        ctx.kU_IfNotExists() ? common::ConflictAction::ON_CONFLICT_DO_NOTHING :
                               common::ConflictAction::ON_CONFLICT_THROW);
    createTableInfo.propertyDefinitions =
        transformPropertyDefinitions(*ctx.kU_PropertyDefinitions());
    createTableInfo.extraInfo = std::make_unique<ExtraCreateNodeTableInfo>(pkName);
    return std::make_unique<CreateTable>(std::move(createTableInfo));
}

std::unique_ptr<Statement> Transformer::transformCreateRelTable(
    CypherParser::KU_CreateRelTableContext& ctx) {
    auto tableName = transformSchemaName(*ctx.oC_SchemaName());
    std::string relMultiplicity = "MANY_MANY";
    if (ctx.oC_SymbolicName()) {
        relMultiplicity = transformSymbolicName(*ctx.oC_SymbolicName());
    }
    auto srcTableName = transformSchemaName(*ctx.kU_RelTableConnection()->oC_SchemaName(0));
    auto dstTableName = transformSchemaName(*ctx.kU_RelTableConnection()->oC_SchemaName(1));
    auto createTableInfo = CreateTableInfo(TableType::REL, tableName,
        ctx.kU_IfNotExists() ? common::ConflictAction::ON_CONFLICT_DO_NOTHING :
                               common::ConflictAction::ON_CONFLICT_THROW);
    if (ctx.kU_PropertyDefinitions()) {
        createTableInfo.propertyDefinitions =
            transformPropertyDefinitions(*ctx.kU_PropertyDefinitions());
    }
    createTableInfo.extraInfo = std::make_unique<ExtraCreateRelTableInfo>(relMultiplicity,
        std::move(srcTableName), std::move(dstTableName));
    return std::make_unique<CreateTable>(std::move(createTableInfo));
}

std::unique_ptr<Statement> Transformer::transformCreateRelTableGroup(
    CypherParser::KU_CreateRelTableGroupContext& ctx) {
    auto tableName = transformSchemaName(*ctx.oC_SchemaName());
    std::vector<std::pair<std::string, std::string>> propertyDefinitions;

    std::string relMultiplicity = "MANY_MANY";
    if (ctx.oC_SymbolicName()) {
        relMultiplicity = transformSymbolicName(*ctx.oC_SymbolicName());
    }
    std::vector<std::pair<std::string, std::string>> srcDstTablePairs;
    for (auto& connection : ctx.kU_RelTableConnection()) {
        auto srcTableName = transformSchemaName(*connection->oC_SchemaName(0));
        auto dstTableName = transformSchemaName(*connection->oC_SchemaName(1));
        srcDstTablePairs.emplace_back(srcTableName, dstTableName);
    }
    auto createTableInfo = CreateTableInfo(TableType::REL_GROUP, tableName,
        ctx.kU_IfNotExists() ? common::ConflictAction::ON_CONFLICT_DO_NOTHING :
                               common::ConflictAction::ON_CONFLICT_THROW);
    if (ctx.kU_PropertyDefinitions()) {
        createTableInfo.propertyDefinitions =
            transformPropertyDefinitions(*ctx.kU_PropertyDefinitions());
    }
    createTableInfo.extraInfo = std::make_unique<ExtraCreateRelTableGroupInfo>(relMultiplicity,
        std::move(srcDstTablePairs));
    return std::make_unique<CreateTable>(std::move(createTableInfo));
}

std::unique_ptr<Statement> Transformer::transformCreateSequence(
    CypherParser::KU_CreateSequenceContext& ctx) {
    auto sequenceName = transformSchemaName(*ctx.oC_SchemaName());
    auto createSequenceInfo = CreateSequenceInfo(sequenceName,
        ctx.kU_IfNotExists() ? common::ConflictAction::ON_CONFLICT_DO_NOTHING :
                               common::ConflictAction::ON_CONFLICT_THROW);
    std::unordered_set<SequenceInfoType> applied;
    for (auto seqOption : ctx.kU_SequenceOptions()) {
        SequenceInfoType type; // NOLINT(*-init-variables)
        std::string typeString;
        CypherParser::OC_IntegerLiteralContext* valCtx = nullptr;
        std::string* valOption = nullptr;
        if (seqOption->kU_StartWith()) {
            type = SequenceInfoType::START;
            typeString = "START";
            valCtx = seqOption->kU_StartWith()->oC_IntegerLiteral();
            valOption = &createSequenceInfo.startWith;
            *valOption = seqOption->kU_StartWith()->MINUS() ? "-" : "";
        } else if (seqOption->kU_IncrementBy()) {
            type = SequenceInfoType::INCREMENT;
            typeString = "INCREMENT";
            valCtx = seqOption->kU_IncrementBy()->oC_IntegerLiteral();
            valOption = &createSequenceInfo.increment;
            *valOption = seqOption->kU_IncrementBy()->MINUS() ? "-" : "";
        } else if (seqOption->kU_MinValue()) {
            type = SequenceInfoType::MINVALUE;
            typeString = "MINVALUE";
            if (!seqOption->kU_MinValue()->NO()) {
                valCtx = seqOption->kU_MinValue()->oC_IntegerLiteral();
                valOption = &createSequenceInfo.minValue;
                *valOption = seqOption->kU_MinValue()->MINUS() ? "-" : "";
            }
        } else if (seqOption->kU_MaxValue()) {
            type = SequenceInfoType::MAXVALUE;
            typeString = "MAXVALUE";
            if (!seqOption->kU_MaxValue()->NO()) {
                valCtx = seqOption->kU_MaxValue()->oC_IntegerLiteral();
                valOption = &createSequenceInfo.maxValue;
                *valOption = seqOption->kU_MaxValue()->MINUS() ? "-" : "";
            }
        } else { // seqOption->kU_Cycle()
            type = SequenceInfoType::CYCLE;
            typeString = "CYCLE";
            if (!seqOption->kU_Cycle()->NO()) {
                createSequenceInfo.cycle = true;
            }
        }
        if (applied.find(type) != applied.end()) {
            throw ParserException(typeString + " should be passed at most once.");
        }
        applied.insert(type);

        if (valCtx && valOption) {
            *valOption += valCtx->DecimalInteger()->getText();
        }
    }
    return std::make_unique<CreateSequence>(std::move(createSequenceInfo));
}

std::unique_ptr<Statement> Transformer::transformCreateType(
    CypherParser::KU_CreateTypeContext& ctx) {
    auto name = transformSchemaName(*ctx.oC_SchemaName());
    auto type = transformDataType(*ctx.kU_DataType());
    return std::make_unique<CreateType>(name, type);
}

DropType transformDropType(CypherParser::KU_DropContext& ctx) {
    if (ctx.TABLE()) {
        return DropType::TABLE;
    } else if (ctx.SEQUENCE()) {
        return DropType::SEQUENCE;
    } else {
        KU_UNREACHABLE;
    }
}

std::unique_ptr<Statement> Transformer::transformDrop(CypherParser::KU_DropContext& ctx) {
    auto name = transformSchemaName(*ctx.oC_SchemaName());
    auto dropType = transformDropType(ctx);
    auto conflictAction = ctx.kU_IfExists() ? common::ConflictAction::ON_CONFLICT_DO_NOTHING :
                                              common::ConflictAction::ON_CONFLICT_THROW;
    return std::make_unique<Drop>(DropInfo{std::move(name), dropType, conflictAction});
}

std::unique_ptr<Statement> Transformer::transformRenameTable(
    CypherParser::KU_AlterTableContext& ctx) {
    auto tableName = transformSchemaName(*ctx.oC_SchemaName());
    auto newName = transformSchemaName(*ctx.kU_AlterOptions()->kU_RenameTable()->oC_SchemaName());
    auto extraInfo = std::make_unique<ExtraRenameTableInfo>(std::move(newName));
    auto info = AlterInfo(AlterType::RENAME_TABLE, tableName, std::move(extraInfo));
    return std::make_unique<Alter>(std::move(info));
}

std::unique_ptr<Statement> Transformer::transformAddProperty(
    CypherParser::KU_AlterTableContext& ctx) {
    auto tableName = transformSchemaName(*ctx.oC_SchemaName());
    auto addPropertyCtx = ctx.kU_AlterOptions()->kU_AddProperty();
    auto propertyName = transformPropertyKeyName(*addPropertyCtx->oC_PropertyKeyName());
    auto dataType = transformDataType(*addPropertyCtx->kU_DataType());
    std::unique_ptr<ParsedExpression> defaultValue;
    if (addPropertyCtx->kU_Default()) {
        defaultValue = transformExpression(*addPropertyCtx->kU_Default()->oC_Expression());
    } else {
        auto type = LogicalType::convertFromString(dataType, context);
        defaultValue =
            std::make_unique<ParsedLiteralExpression>(Value::createNullValue(type), "NULL");
    }
    auto extraInfo = std::make_unique<ExtraAddPropertyInfo>(std::move(propertyName),
        std::move(dataType), std::move(defaultValue));
    auto info = AlterInfo(AlterType::ADD_PROPERTY, tableName, std::move(extraInfo));
    return std::make_unique<Alter>(std::move(info));
}

std::unique_ptr<Statement> Transformer::transformDropProperty(
    CypherParser::KU_AlterTableContext& ctx) {
    auto tableName = transformSchemaName(*ctx.oC_SchemaName());
    auto propertyName =
        transformPropertyKeyName(*ctx.kU_AlterOptions()->kU_DropProperty()->oC_PropertyKeyName());
    auto extraInfo = std::make_unique<ExtraDropPropertyInfo>(std::move(propertyName));
    auto info = AlterInfo(AlterType::DROP_PROPERTY, tableName, std::move(extraInfo));
    return std::make_unique<Alter>(std::move(info));
}

std::unique_ptr<Statement> Transformer::transformRenameProperty(
    CypherParser::KU_AlterTableContext& ctx) {
    auto tableName = transformSchemaName(*ctx.oC_SchemaName());
    auto propertyName = transformPropertyKeyName(
        *ctx.kU_AlterOptions()->kU_RenameProperty()->oC_PropertyKeyName()[0]);
    auto newName = transformPropertyKeyName(
        *ctx.kU_AlterOptions()->kU_RenameProperty()->oC_PropertyKeyName()[1]);
    auto extraInfo = std::make_unique<ExtraRenamePropertyInfo>(propertyName, newName);
    auto info = AlterInfo(AlterType::RENAME_PROPERTY, tableName, std::move(extraInfo));
    return std::make_unique<Alter>(std::move(info));
}

std::unique_ptr<Statement> Transformer::transformCommentOn(CypherParser::KU_CommentOnContext& ctx) {
    auto tableName = transformSchemaName(*ctx.oC_SchemaName());
    auto comment = transformStringLiteral(*ctx.StringLiteral());
    auto extraInfo = std::make_unique<ExtraCommentInfo>(comment);
    auto info = AlterInfo(AlterType::COMMENT, tableName, std::move(extraInfo));
    return std::make_unique<Alter>(std::move(info));
}

std::vector<ParsedColumnDefinition> Transformer::transformColumnDefinitions(
    CypherParser::KU_ColumnDefinitionsContext& ctx) {
    std::vector<ParsedColumnDefinition> definitions;
    for (auto& definition : ctx.kU_ColumnDefinition()) {
        definitions.emplace_back(transformColumnDefinition(*definition));
    }
    return definitions;
}

ParsedColumnDefinition Transformer::transformColumnDefinition(
    CypherParser::KU_ColumnDefinitionContext& ctx) {
    auto propertyName = transformPropertyKeyName(*ctx.oC_PropertyKeyName());
    auto dataType = transformDataType(*ctx.kU_DataType());
    return ParsedColumnDefinition(propertyName, dataType);
}

std::vector<ParsedPropertyDefinition> Transformer::transformPropertyDefinitions(
    CypherParser::KU_PropertyDefinitionsContext& ctx) {
    std::vector<ParsedPropertyDefinition> definitions;
    for (auto& definition : ctx.kU_PropertyDefinition()) {
        auto columnDefinition = transformColumnDefinition(*definition->kU_ColumnDefinition());
        std::unique_ptr<ParsedExpression> defaultExpr;
        if (definition->kU_Default()) {
            defaultExpr = transformExpression(*definition->kU_Default()->oC_Expression());
        } else {
            auto type = LogicalType::convertFromString(columnDefinition.type, context);
            defaultExpr = std::make_unique<ParsedLiteralExpression>(
                Value::createNullValue(std::move(type)), "NULL");
        }
        definitions.push_back(
            ParsedPropertyDefinition(std::move(columnDefinition), std::move(defaultExpr)));
    }
    return definitions;
}

std::string Transformer::transformDataType(CypherParser::KU_DataTypeContext& ctx) {
    return ctx.getText();
}

std::string Transformer::transformPrimaryKey(CypherParser::KU_CreateNodeConstraintContext& ctx) {
    return transformPropertyKeyName(*ctx.oC_PropertyKeyName());
}

std::string Transformer::transformPrimaryKey(CypherParser::KU_ColumnDefinitionContext& ctx) {
    return transformPropertyKeyName(*ctx.oC_PropertyKeyName());
}

} // namespace parser
} // namespace kuzu
