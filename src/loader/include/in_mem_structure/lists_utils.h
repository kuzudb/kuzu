#pragma once

#include <atomic>
#include <memory>
#include <unordered_map>

#include "robin_hood.h"

#include "src/common/include/compression_scheme.h"
#include "src/common/include/configs.h"
#include "src/common/include/types.h"
#include "src/common/include/utils.h"
#include "src/loader/include/dataset_metadata.h"
#include "src/storage/include/catalog.h"
#include "src/storage/include/data_structure/lists/list_headers.h"
#include "src/storage/include/data_structure/lists/lists_metadata.h"
#include "src/storage/include/data_structure/lists/utils.h"

using namespace std;
using namespace graphflow::common;
using namespace graphflow::storage;

namespace graphflow {
namespace loader {

// listSizes_t is the type of structure that is used to count the size of each list in the
// particular Lists data structure.
typedef vector<atomic<uint64_t>> listSizes_t;

// Helper functions to assist populating ListHeaders, ListsMetadata and in-memory representation of
// Lists.
class ListsUtils {

public:
    static inline void incrementListSize(listSizes_t& listSizes, uint32_t offset, uint32_t val) {
        listSizes[offset].fetch_add(val, memory_order_relaxed);
    }

    static inline uint64_t decrementListSize(
        listSizes_t& listSizes, uint32_t offset, uint32_t val) {
        return listSizes[offset].fetch_sub(val, memory_order_relaxed);
    }

    // Inits (in listHeaders) the header of each list in a Lists structure, from the listSizes.
    // ListSizes is used to determine if the list is small or large, based on which, information is
    // encoded in the 4 byte header.
    static void calculateListHeadersTask(node_offset_t numNodeOffsets, uint32_t numPerPage,
        listSizes_t* listSizes, ListHeaders* listHeaders, const shared_ptr<spdlog::logger>& logger);

    // Initializes Metadata information of a Lists structure, that is chunksPagesMap and
    // largeListsPagesMap, using listSizes and listHeaders.
    static void calculateListsMetadataTask(uint64_t numNodeOffsets, uint32_t numPerPage,
        listSizes_t* listSizes, ListHeaders* listHeaders, ListsMetadata* listsMetadata,
        const shared_ptr<spdlog::logger>& logger);

    // Calculates the page id and offset in page where the data of a particular list has to be put
    // in the in-mem pages.
    static void calculatePageCursor(uint32_t header, uint64_t reversePos,
        uint8_t numBytesPerElement, node_offset_t nodeOffset, PageCursor& cursor,
        ListsMetadata& metadata);
};

} // namespace loader
} // namespace graphflow
