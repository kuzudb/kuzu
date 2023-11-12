#include "parser/copy.h"
#include "parser/transformer.h"

using namespace kuzu::common;

namespace kuzu {
namespace parser {

std::unique_ptr<Statement> Transformer::transformCopyTo(CypherParser::KU_CopyTOContext& ctx) {
    std::string filePath = transformStringLiteral(*ctx.StringLiteral());
    auto regularQuery = transformQuery(*ctx.oC_Query());
    auto copyTo = std::make_unique<CopyTo>(std::move(filePath), std::move(regularQuery));
    if (ctx.kU_ParsingOptions()) {
        copyTo->setParsingOption(transformParsingOptions(*ctx.kU_ParsingOptions()));
    }
    return copyTo;
}

std::unique_ptr<Statement> Transformer::transformCopyFrom(CypherParser::KU_CopyFromContext& ctx) {
    auto filePaths = transformFilePaths(ctx.kU_FilePaths()->StringLiteral());
    auto tableName = transformSchemaName(*ctx.oC_SchemaName());
    auto copyFrom = std::make_unique<CopyFrom>(std::move(filePaths), std::move(tableName));
    if (ctx.kU_ColumnNames()) {
        copyFrom->setColumnNames(transformColumnNames(*ctx.kU_ColumnNames()));
    }
    if (ctx.kU_ParsingOptions()) {
        copyFrom->setParsingOption(transformParsingOptions(*ctx.kU_ParsingOptions()));
    }
    return copyFrom;
}

std::unique_ptr<Statement> Transformer::transformCopyFromByColumn(
    CypherParser::KU_CopyFromByColumnContext& ctx) {
    auto filePaths = transformFilePaths(ctx.StringLiteral());
    auto tableName = transformSchemaName(*ctx.oC_SchemaName());
    auto copyFrom = std::make_unique<CopyFrom>(std::move(filePaths), std::move(tableName));
    copyFrom->setByColumn();
    return copyFrom;
}

std::vector<std::string> Transformer::transformColumnNames(
    CypherParser::KU_ColumnNamesContext& ctx) {
    std::vector<std::string> columnNames;
    for (auto& schemaName : ctx.oC_SchemaName()) {
        columnNames.push_back(transformSchemaName(*schemaName));
    }
    return columnNames;
}

std::vector<std::string> Transformer::transformFilePaths(
    std::vector<antlr4::tree::TerminalNode*> stringLiteral) {
    std::vector<std::string> csvFiles;
    for (auto& csvFile : stringLiteral) {
        csvFiles.push_back(transformStringLiteral(*csvFile));
    }
    return csvFiles;
}

std::unordered_map<std::string, std::unique_ptr<ParsedExpression>>
Transformer::transformParsingOptions(CypherParser::KU_ParsingOptionsContext& ctx) {
    std::unordered_map<std::string, std::unique_ptr<ParsedExpression>> copyOptions;
    for (auto loadOption : ctx.kU_ParsingOption()) {
        auto optionName = transformSymbolicName(*loadOption->oC_SymbolicName());
        copyOptions.emplace(optionName, transformLiteral(*loadOption->oC_Literal()));
    }
    return copyOptions;
}

} // namespace parser
} // namespace kuzu
