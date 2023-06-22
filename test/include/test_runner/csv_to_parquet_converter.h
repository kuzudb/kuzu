#include "main/kuzu.h"
#include <arrow/api.h>

namespace kuzu {
namespace testing {

// Convert an entire CSV dataset directory to parquet.
// The dataset directory must contain schema and copy files.
class CSVToParquetConverter {
public:
    static std::string convertCSVDatasetToParquet(
        const std::string& csvDatasetPath, const std::string& parquetDatasetPath);

    inline static std::string replaceSlashesWithUnderscores(std::string dataset) {
        std::replace(dataset.begin(), dataset.end(), '/', '_');
        return dataset;
    }

private:
    struct CopyCommandInfo {
        std::string table;
        std::string csvFilePath;
        std::string parquetFilePath;
        bool csvHasHeader;
        char delimiter;
    };

    static std::vector<CopyCommandInfo> readCopyCommandsFromCopyCypherFile(
        const std::string& csvDatasetPath, const std::string& parquetDatasetPath);

    static void convertCSVFilesToParquet(
        const std::vector<CSVToParquetConverter::CopyCommandInfo>& copyCommands);

    static CopyCommandInfo createCopyCommandInfo(
        const std::string& parquetDatasetPath, std::string copyStatement);

    static arrow::Status runCSVToParquetConversion(const std::string& inputFile,
        const std::string& outputFile, char delimiter, bool hasHeader);

    static void copySchema(
        const std::string& csvDatasetPath, const std::string& parquetDatasetPath);

    static void createCopyFile(const std::string& parquetDatasetPath,
        const std::vector<CSVToParquetConverter::CopyCommandInfo>& copyCommands);
};

} // namespace testing
} // namespace kuzu
