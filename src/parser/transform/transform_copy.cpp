#include "common/assert.h"
#include "parser/copy.h"
#include "parser/scan_source.h"
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
    auto source = transformScanSource(*ctx.kU_ScanSource());
    auto tableName = transformSchemaName(*ctx.oC_SchemaName());
    auto copyFrom = std::make_unique<CopyFrom>(std::move(source), std::move(tableName));
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
    auto source = std::make_unique<FileScanSource>(transformFilePaths(ctx.StringLiteral()));
    auto tableName = transformSchemaName(*ctx.oC_SchemaName());
    auto copyFrom = std::make_unique<CopyFrom>(std::move(source), std::move(tableName));
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
    const std::vector<antlr4::tree::TerminalNode*>& stringLiteral) {
    std::vector<std::string> csvFiles;
    csvFiles.reserve(stringLiteral.size());
    for (auto& csvFile : stringLiteral) {
        csvFiles.push_back(transformStringLiteral(*csvFile));
    }
    return csvFiles;
}

std::unique_ptr<BaseScanSource> Transformer::transformScanSource(
    CypherParser::KU_ScanSourceContext& ctx) {
    if (ctx.kU_FilePaths()) {
        auto filePaths = transformFilePaths(ctx.kU_FilePaths()->StringLiteral());
        return std::make_unique<FileScanSource>(std::move(filePaths));
    } else if (ctx.oC_Query()) {
        auto query = transformQuery(*ctx.oC_Query());
        return std::make_unique<QueryScanSource>(std::move(query));
    } else if (ctx.oC_Variable()) {
        std::vector<std::string> objectNames;
        objectNames.push_back(transformVariable(*ctx.oC_Variable()));
        if (ctx.oC_SchemaName()) {
            objectNames.push_back(transformSchemaName(*ctx.oC_SchemaName()));
        }
        return std::make_unique<ObjectScanSource>(std::move(objectNames));
    }
    KU_UNREACHABLE;
}

parsing_option_t Transformer::transformParsingOptions(CypherParser::KU_ParsingOptionsContext& ctx) {
    std::unordered_map<std::string, std::unique_ptr<ParsedExpression>> copyOptions;
    for (auto loadOption : ctx.kU_ParsingOption()) {
        auto optionName = transformSymbolicName(*loadOption->oC_SymbolicName());
        copyOptions.emplace(optionName, transformLiteral(*loadOption->oC_Literal()));
    }
    return copyOptions;
}

} // namespace parser
} // namespace kuzu
