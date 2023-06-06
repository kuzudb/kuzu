#include "main/kuzu.h"
#include <arrow/api.h>

namespace kuzu {
namespace testing {

// Convert an entire CSV dataset directory to parquet.
// The dataset directory must contain schema and copy files.
class CSVToParquetConverter {
public:
    static void convertCSVDatasetToParquet(std::string& dataset);

private:
    struct CopyCommandInfo {
        std::string table;
        std::string csvFilePath;
        std::string parquetFilePath;
        bool csvHasHeader;
        char delimiter;
    };

    static std::vector<CopyCommandInfo> readCopyCommandsFromCopyCypherFile(
        const std::string& dataset);

    static void convertCSVFilesToParquet(
        const std::vector<CSVToParquetConverter::CopyCommandInfo>& copyCommands);

    static CopyCommandInfo createCopyCommandInfo(
        const std::string& dataset, std::string copyStatement);

    static arrow::Status runCSVToParquetConversion(const std::string& inputFile,
        const std::string& outputFile, char delimiter, bool hasHeader);

    static void copySchema(
        const std::string& csvDatasetPath, const std::string& parquetDatasetPath);

    static void createCopyFile(const std::string& dataset,
        const std::vector<CSVToParquetConverter::CopyCommandInfo>& copyCommands);

    inline static std::string replaceSlashesWithUnderscores(std::string dataset) {
        std::replace(dataset.begin(), dataset.end(), '/', '_');
        return dataset;
    }

    static std::string extractPath(std::string& str, char delimiter);
};

} // namespace testing
} // namespace kuzu
