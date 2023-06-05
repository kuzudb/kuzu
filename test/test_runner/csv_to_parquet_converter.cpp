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

arrow::Status CSVToParquetConverter::RunCSVToParquetConversion(
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
    auto parquetSchemaFile = parquetDatasetPath + "/" + std::string(TestHelper::SCHEMA_FILE_NAME);
    if (!FileUtils::fileOrPathExists(parquetSchemaFile)) {
        try {
            std::filesystem::copy(csvDatasetPath + "/schema.cypher", parquetDatasetPath);
        } catch (std::filesystem::filesystem_error& e) {
            throw TestException("Failed to copy schema cypher files to parquet temp dir.");
        }
    }
}

CSVToParquetConverter::CopyCommandInfo CSVToParquetConverter::createCopyCommandInfo(
    const std::string& dataset, const std::string& copyStatement) {
    auto tokens = StringUtils::split(copyStatement, " ");
    auto path = std::filesystem::path(StringUtils::extractSubstring(tokens[3], '"'));
    CopyCommandInfo copyCommandInfo;
    copyCommandInfo.table = tokens[1];
    copyCommandInfo.csvFilePath = TestHelper::appendKuzuRootPath(path.string());
    copyCommandInfo.parquetFilePath = TestHelper::appendParquetDatasetTempDir(
        replaceSlashesWithUnderscores(dataset) + "/" + path.stem().string() + ".parquet");
    copyCommandInfo.csvHasHeader = (copyStatement.find("HEADER=true") != std::string::npos);
    std::size_t delimPos = copyStatement.find("DELIM='");
    copyCommandInfo.delimiter = (delimPos != std::string::npos) ? copyStatement[delimPos + 7] : ',';
    return copyCommandInfo;
}

std::vector<CSVToParquetConverter::CopyCommandInfo>
CSVToParquetConverter::readCopyCommandsFromCopyCypherFile(const std::string& dataset) {
    auto copyFile =
        TestHelper::appendKuzuRootPath("dataset/" + dataset + "/" + TestHelper::COPY_FILE_NAME);
    std::ifstream file(copyFile);
    if (!file.is_open()) {
        throw TestException(
            StringUtils::string_format("Error opening file: {}, errno: {}.", copyFile, errno));
    }
    std::string line;
    std::vector<CopyCommandInfo> copyCommands;
    while (getline(file, line)) {
        copyCommands.push_back(createCopyCommandInfo(dataset, line));
    }
    return copyCommands;
}

void CSVToParquetConverter::createCopyFile(const std::string& dataset,
    const std::vector<CSVToParquetConverter::CopyCommandInfo>& copyCommands) {
    auto parquetDatasetPath =
        TestHelper::appendParquetDatasetTempDir(replaceSlashesWithUnderscores(dataset));
    auto targetCopyCypherFile = parquetDatasetPath + "/" + TestHelper::COPY_FILE_NAME;
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
            RunCSVToParquetConversion(copyCommand.csvFilePath, copyCommand.parquetFilePath,
                copyCommand.delimiter, copyCommand.csvHasHeader);
        }
    }
}

void CSVToParquetConverter::convertCSVDatasetToParquet(std::string& dataset) {
    auto csvDatasetPath = TestHelper::appendKuzuRootPath("dataset/" + dataset);
    auto parquetDatasetPath =
        TestHelper::appendParquetDatasetTempDir(replaceSlashesWithUnderscores(dataset));
    FileUtils::createDirIfNotExists(parquetDatasetPath);
    std::vector<CSVToParquetConverter::CopyCommandInfo> copyCommands =
        readCopyCommandsFromCopyCypherFile(dataset);
    copySchema(csvDatasetPath, parquetDatasetPath);
    createCopyFile(dataset, copyCommands);
    convertCSVFilesToParquet(copyCommands);
    dataset = parquetDatasetPath;
}

} // namespace testing
} // namespace kuzu
