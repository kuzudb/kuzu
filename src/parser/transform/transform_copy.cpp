#include "parser/copy.h"
#include "parser/transformer.h"

using namespace kuzu::common;

namespace kuzu {
namespace parser {

std::unique_ptr<Statement> Transformer::transformCopyTo(CypherParser::KU_CopyTOContext& ctx) {
    std::string filePath = transformStringLiteral(*ctx.StringLiteral());
    auto regularQuery = transformQuery(*ctx.oC_Query());
    return std::make_unique<CopyTo>(std::move(filePath), std::move(regularQuery));
}

std::unique_ptr<Statement> Transformer::transformCopyFrom(CypherParser::KU_CopyFromContext& ctx) {
    auto filePaths = transformFilePaths(ctx.kU_FilePaths()->StringLiteral());
    auto tableName = transformSchemaName(*ctx.oC_SchemaName());
    std::unordered_map<std::string, std::unique_ptr<ParsedExpression>> parsingOptions;
    if (ctx.kU_ParsingOptions()) {
        parsingOptions = transformParsingOptions(*ctx.kU_ParsingOptions());
    }
    return std::make_unique<CopyFrom>(false /* byColumn */, std::move(filePaths),
        std::move(tableName), std::move(parsingOptions));
}

std::unique_ptr<Statement> Transformer::transformCopyFromByColumn(
    CypherParser::KU_CopyFromByColumnContext& ctx) {
    auto filePaths = transformFilePaths(ctx.StringLiteral());
    auto tableName = transformSchemaName(*ctx.oC_SchemaName());
    auto parsingOptions = std::unordered_map<std::string, std::unique_ptr<ParsedExpression>>();
    return std::make_unique<CopyFrom>(
        true /* byColumn */, std::move(filePaths), std::move(tableName), std::move(parsingOptions));
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
