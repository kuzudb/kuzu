#include "test_runner/csv_to_parquet_converter.h"

#include <fstream>

#include "common/file_utils.h"
#include "common/string_utils.h"
#include "spdlog/spdlog.h"
#include "test_helper/test_helper.h"
#include <arrow/csv/api.h>
#include <arrow/io/api.h>
#include <arrow/ipc/api.h>
#include <parquet/arrow/reader.h>
#include <parquet/arrow/writer.h>

using namespace kuzu::common;

namespace kuzu {
namespace testing {

arrow::Status CSVToParquetConverter::runCSVToParquetConversion(
    const std::string& inputFile, const std::string& outputFile, char delimiter, bool hasHeader) {
    std::shared_ptr<arrow::io::FileOutputStream> outFileStream;
    std::shared_ptr<arrow::io::ReadableFile> infile;
    std::shared_ptr<arrow::Table> csvTable;
    ARROW_ASSIGN_OR_RAISE(infile, arrow::io::ReadableFile::Open(inputFile));
    auto readOptions = arrow::csv::ReadOptions::Defaults();
    auto parseOptions = arrow::csv::ParseOptions::Defaults();
    readOptions.autogenerate_column_names = !hasHeader;
    parseOptions.delimiter = delimiter;
    ARROW_ASSIGN_OR_RAISE(
        auto csvReader, arrow::csv::TableReader::Make(arrow::io::default_io_context(), infile,
                            readOptions, parseOptions, arrow::csv::ConvertOptions::Defaults()));
    ARROW_ASSIGN_OR_RAISE(csvTable, csvReader->Read());
    ARROW_ASSIGN_OR_RAISE(outFileStream, arrow::io::FileOutputStream::Open(outputFile));
    PARQUET_THROW_NOT_OK(parquet::arrow::WriteTable(
        *csvTable, arrow::default_memory_pool(), outFileStream, csvTable->num_rows()));
    return arrow::Status::OK();
}

void CSVToParquetConverter::copySchema(
    const std::string& csvDatasetPath, const std::string& parquetDatasetPath) {
    auto parquetSchemaFile =
        FileUtils::joinPath(parquetDatasetPath, std::string(TestHelper::SCHEMA_FILE_NAME));
    FileUtils::copyFile(FileUtils::joinPath(csvDatasetPath, "schema.cypher"),
        FileUtils::joinPath(parquetDatasetPath, "schema.cypher"),
        std::filesystem::copy_options::overwrite_existing);
}

CSVToParquetConverter::CopyCommandInfo CSVToParquetConverter::createCopyCommandInfo(
    const std::string& parquetDatasetPath, std::string copyStatement) {
    auto tokens = StringUtils::split(copyStatement, " ");
    auto path = std::filesystem::path(StringUtils::extractStringBetween(tokens[3], '"', '"'));
    CopyCommandInfo copyCommandInfo;
    copyCommandInfo.table = tokens[1];
    copyCommandInfo.csvFilePath = TestHelper::appendKuzuRootPath(path.string());
    copyCommandInfo.parquetFilePath = parquetDatasetPath + "/" + path.stem().string() + ".parquet";
    std::transform(copyStatement.begin(), copyStatement.end(), copyStatement.begin(),
        [](unsigned char c) { return std::tolower(c); });
    copyCommandInfo.csvHasHeader = (copyStatement.find("header=true") != std::string::npos);
    std::size_t delimPos = copyStatement.find("delim='");
    copyCommandInfo.delimiter = (delimPos != std::string::npos) ? copyStatement[delimPos + 7] : ',';
    return copyCommandInfo;
}

std::vector<CSVToParquetConverter::CopyCommandInfo>
CSVToParquetConverter::readCopyCommandsFromCopyCypherFile(
    const std::string& csvDatasetPath, const std::string& parquetDatasetPath) {
    auto copyFile = FileUtils::joinPath(csvDatasetPath, TestHelper::COPY_FILE_NAME);
    std::ifstream file(copyFile);
    if (!file.is_open()) {
        throw TestException(
            StringUtils::string_format("Error opening file: {}, errno: {}.", copyFile, errno));
    }
    std::string line;
    std::vector<CopyCommandInfo> copyCommands;
    while (getline(file, line)) {
        copyCommands.push_back(createCopyCommandInfo(parquetDatasetPath, line));
    }
    return copyCommands;
}

void CSVToParquetConverter::createCopyFile(const std::string& parquetDatasetPath,
    const std::vector<CSVToParquetConverter::CopyCommandInfo>& copyCommands) {
    auto targetCopyCypherFile = FileUtils::joinPath(parquetDatasetPath, TestHelper::COPY_FILE_NAME);
    std::ofstream outfile(targetCopyCypherFile);
    if (!outfile.is_open()) {
        throw TestException(StringUtils::string_format(
            "Error opening file: {}, errno: {}.", targetCopyCypherFile, errno));
    }
    for (auto copyCommand : copyCommands) {
        auto cmd = "COPY " + copyCommand.table + " FROM '" + copyCommand.parquetFilePath + "'";
        outfile << cmd << std::endl;
    }
}

void CSVToParquetConverter::convertCSVFilesToParquet(
    const std::vector<CSVToParquetConverter::CopyCommandInfo>& copyCommands) {
    for (auto copyCommand : copyCommands) {
        if (!FileUtils::fileOrPathExists(copyCommand.parquetFilePath)) {
            spdlog::info(
                "CONVERTING: {} to {}", copyCommand.csvFilePath, copyCommand.parquetFilePath);
            runCSVToParquetConversion(copyCommand.csvFilePath, copyCommand.parquetFilePath,
                copyCommand.delimiter, copyCommand.csvHasHeader);
        }
    }
}

std::string CSVToParquetConverter::convertCSVDatasetToParquet(
    const std::string& csvDatasetPath, const std::string& parquetDatasetPath) {
    FileUtils::createDirIfNotExists(parquetDatasetPath);
    std::vector<CSVToParquetConverter::CopyCommandInfo> copyCommands =
        readCopyCommandsFromCopyCypherFile(csvDatasetPath, parquetDatasetPath);
    copySchema(csvDatasetPath, parquetDatasetPath);
    createCopyFile(parquetDatasetPath, copyCommands);
    convertCSVFilesToParquet(copyCommands);
    return parquetDatasetPath;
}

} // namespace testing
} // namespace kuzu
