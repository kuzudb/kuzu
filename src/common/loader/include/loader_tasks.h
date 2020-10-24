#ifndef GRAPHFLOW_COMMON_LOADER_LOADER_TASKS_H_
#define GRAPHFLOW_COMMON_LOADER_LOADER_TASKS_H_

#include <mutex>
#include <string>
#include <vector>

#include "spdlog/spdlog.h"

#include "src/common/loader/include/csv_reader.h"
#include "src/common/loader/include/utils.h"

using namespace std;

namespace graphflow {
namespace common {

class LoaderTasks {
public:
    static void fileBlockLinesCounterTask(string fname, char tokenSeparator,
        vector<vector<uint64_t>> *numLinesPerBlock, gfLabel_t label, uint32_t blockId,
        shared_ptr<spdlog::logger> logger);
    static void NodePropertyReaderTask(string fname, char tokenSeparator,
        vector<Property> &propertyMap, uint32_t blockId, gfNodeOffset_t offset,
        vector<unique_ptr<InMemoryColumnBase>> *inMemoryColumns, shared_ptr<spdlog::logger> logger);
    static void ColumnSerializer(
        const string fname, InMemoryColumnBase *inMemoryColumn, shared_ptr<spdlog::logger> logger);
};

} // namespace common
} // namespace graphflow

#endif // GRAPHFLOW_COMMON_LOADER_LOADER_TASKS_H_
