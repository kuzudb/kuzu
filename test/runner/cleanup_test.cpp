#include "graph_test/graph_test.h"

using namespace kuzu::testing;
using namespace kuzu::common;

void deleteMatchingDir(const std::string& dirPath, const std::string& match) {
    for (const auto& entry : std::filesystem::directory_iterator(dirPath)) {
        if (entry.path().filename().string().find(match) != std::string::npos) {
            FileUtils::removeDir(entry.path().string());
        }
    }
}

int main(int argc, char** argv) {
    std::vector<std::string> tempDirs = {
        TestHelper::PARQUET_TEMP_DATASET_PATH, TestHelper::TMP_TEST_DIR};
    if (argc > 1 && std::string(argv[1]) == "--gtest_list_tests") {
        for (const auto& tempDir : tempDirs) {
            // path = test/unittest_temp_
            std::filesystem::path path = tempDir;
            // dirToCheck = test
            std::string dirToCheck = path.parent_path().string();
            // dirPatternToRemove = unittest_temp_
            std::string dirPatternToRemove = path.filename().string();
            deleteMatchingDir(TestHelper::appendKuzuRootPath(dirToCheck), dirPatternToRemove);
        }
    }
    return 0;
}
