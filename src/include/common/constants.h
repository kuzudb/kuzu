#pragma once

#include <cstdint>
#include <string>
#include <unordered_map>

namespace kuzu {
namespace common {

constexpr uint64_t DEFAULT_VECTOR_CAPACITY_LOG_2 = 11;
constexpr uint64_t DEFAULT_VECTOR_CAPACITY = (uint64_t)1 << DEFAULT_VECTOR_CAPACITY_LOG_2;

constexpr double DEFAULT_HT_LOAD_FACTOR = 1.5;
constexpr uint32_t VAR_LENGTH_EXTEND_MAX_DEPTH = 30;

// This is the default thread sleep time we use when a thread,
// e.g., a worker thread is in TaskScheduler, needs to block.
constexpr uint64_t THREAD_SLEEP_TIME_WHEN_WAITING_IN_MICROS = 500;

constexpr uint64_t DEFAULT_CHECKPOINT_WAIT_TIMEOUT_FOR_TRANSACTIONS_TO_LEAVE_IN_MICROS = 5000000;

struct InternalKeyword {
    static constexpr char ANONYMOUS[] = "";
    static constexpr char ID[] = "_id";
    static constexpr char LENGTH[] = "_length";
    static constexpr char NODES[] = "_nodes";
    static constexpr char RELS[] = "_rels";
    static constexpr char TAG[] = "_tag";
};

enum PageSizeClass : uint8_t {
    PAGE_4KB = 0,
    PAGE_256KB = 1,
};

// Currently the system supports files with 2 different pages size, which we refer to as
// PAGE_4KB_SIZE and PAGE_256KB_SIZE. PAGE_4KB_SIZE is the default size of the page which is the
// unit of read/write to the database files, such as to store columns or lists. For now, this value
// cannot be changed. But technically it can change from 2^12 to 2^16. 2^12 lower bound is assuming
// the OS page size is 4K. 2^16 is because currently we leave 11 fixed number of bits for
// relOffInPage and the maximum number of bytes needed for an edge is 20 bytes so 11 + log_2(20)
// = 15.xxx, so certainly over 2^16-size pages, we cannot utilize the page for storing adjacency
// lists.
struct BufferPoolConstants {
    static constexpr uint64_t PAGE_4KB_SIZE_LOG2 = 12;
    static constexpr uint64_t PAGE_4KB_SIZE = (std::uint64_t)1 << PAGE_4KB_SIZE_LOG2;
    // Page size for files with large pages, e.g., temporary files that are used by operators that
    // may require large amounts of memory.
    static constexpr uint64_t PAGE_256KB_SIZE_LOG2 = 18;
    static constexpr uint64_t PAGE_256KB_SIZE = (std::uint64_t)1 << PAGE_256KB_SIZE_LOG2;
    // If a user does not specify a max size for BM, we by default set the max size of BM to
    // maxPhyMemSize * DEFAULT_PHY_MEM_SIZE_RATIO_FOR_BM.
    static constexpr double DEFAULT_PHY_MEM_SIZE_RATIO_FOR_BM = 0.8;
    // For each PURGE_EVICTION_QUEUE_INTERVAL candidates added to the eviction queue, we will call
    // `removeNonEvictableCandidates` to remove candidates that are not evictable. See
    // `EvictionQueue::removeNonEvictableCandidates()` for more details.
    static constexpr uint64_t EVICTION_QUEUE_PURGING_INTERVAL = 1024;
    // The default max size for a VMRegion.
    static constexpr uint64_t DEFAULT_VM_REGION_MAX_SIZE = (uint64_t)1 << 43; // (8TB)

    static constexpr uint64_t DEFAULT_BUFFER_POOL_SIZE_FOR_TESTING = 1ull << 26; // (64MB)
};

struct StorageConstants {
    static constexpr char OVERFLOW_FILE_SUFFIX[] = ".ovf";
    static constexpr char COLUMN_FILE_SUFFIX[] = ".col";
    static constexpr char LISTS_FILE_SUFFIX[] = ".lists";
    static constexpr char WAL_FILE_SUFFIX[] = ".wal";
    static constexpr char INDEX_FILE_SUFFIX[] = ".hindex";
    static constexpr char NODES_STATISTICS_AND_DELETED_IDS_FILE_NAME[] =
        "nodes.statistics_and_deleted.ids";
    static constexpr char NODES_STATISTICS_FILE_NAME_FOR_WAL[] =
        "nodes.statistics_and_deleted.ids.wal";
    static constexpr char RELS_METADATA_FILE_NAME[] = "rels.statistics";
    static constexpr char RELS_METADATA_FILE_NAME_FOR_WAL[] = "rels.statistics.wal";
    static constexpr char CATALOG_FILE_NAME[] = "catalog.bin";
    static constexpr char CATALOG_FILE_NAME_FOR_WAL[] = "catalog.bin.wal";

    // The number of pages that we add at one time when we need to grow a file.
    static constexpr uint64_t PAGE_GROUP_SIZE_LOG2 = 10;
    static constexpr uint64_t PAGE_GROUP_SIZE = (uint64_t)1 << PAGE_GROUP_SIZE_LOG2;
    static constexpr uint64_t PAGE_IDX_IN_GROUP_MASK = ((uint64_t)1 << PAGE_GROUP_SIZE_LOG2) - 1;
};

struct ListsMetadataConstants {
    // LIST_CHUNK_SIZE should strictly be a power of 2.
    constexpr static uint16_t LISTS_CHUNK_SIZE_LOG_2 = 9;
    constexpr static uint16_t LISTS_CHUNK_SIZE = 1 << LISTS_CHUNK_SIZE_LOG_2;
    // All pageLists (defined later) are broken in pieces (called a pageListGroups) of size
    // PAGE_LIST_GROUP_SIZE + 1 each and chained together. In each piece, there are
    // PAGE_LIST_GROUP_SIZE elements of that list and the offset to the next pageListGroups in the
    // blob that contains all pageListGroups of all lists.
    static constexpr uint32_t PAGE_LIST_GROUP_SIZE = 20;
    static constexpr uint32_t PAGE_LIST_GROUP_WITH_NEXT_PTR_SIZE = PAGE_LIST_GROUP_SIZE + 1;
};

// Hash Index Configurations
struct HashIndexConstants {
    static constexpr uint8_t SLOT_CAPACITY = 3;
};

struct CopyConstants {
    // Size (in bytes) of the chunks to be read in Node/Rel Copier
    static constexpr uint64_t CSV_READING_BLOCK_SIZE = 1 << 23;

    // Number of tasks to be assigned in a batch when reading files.
    static constexpr uint64_t NUM_COPIER_TASKS_TO_SCHEDULE_PER_BATCH = 200;

    // Lower bound for number of incomplete tasks in copier to trigger scheduling a new batch.
    static constexpr uint64_t MINIMUM_NUM_COPIER_TASKS_TO_SCHEDULE_MORE = 50;

    // Number of rows per block for npy files
    static constexpr uint64_t NUM_ROWS_PER_BLOCK_FOR_NPY = 2048;

    // Default configuration for csv file parsing
    static constexpr const char* STRING_CSV_PARSING_OPTIONS[5] = {
        "ESCAPE", "DELIM", "QUOTE", "LIST_BEGIN", "LIST_END"};
    static constexpr char DEFAULT_CSV_ESCAPE_CHAR = '\\';
    static constexpr char DEFAULT_CSV_DELIMITER = ',';
    static constexpr char DEFAULT_CSV_QUOTE_CHAR = '"';
    static constexpr char DEFAULT_CSV_LIST_BEGIN_CHAR = '[';
    static constexpr char DEFAULT_CSV_LIST_END_CHAR = ']';
    static constexpr bool DEFAULT_CSV_HAS_HEADER = false;
};

struct LoggerConstants {
    enum class LoggerEnum : uint8_t {
        DATABASE = 0,
        CSV_READER = 1,
        LOADER = 2,
        PROCESSOR = 3,
        BUFFER_MANAGER = 4,
        CATALOG = 5,
        STORAGE = 6,
        TRANSACTION_MANAGER = 7,
        WAL = 8,
    };
};

struct PlannerKnobs {
    static constexpr double NON_EQUALITY_PREDICATE_SELECTIVITY = 0.1;
    static constexpr double EQUALITY_PREDICATE_SELECTIVITY = 0.01;
    static constexpr uint64_t BUILD_PENALTY = 2;
    // Avoid doing probe to build SIP if we have to accumulate a probe side that is much bigger than
    // build side. Also avoid doing build to probe SIP if probe side is not much bigger than build.
    static constexpr uint64_t SIP_RATIO = 5;
};

struct ClientContextConstants {
    // We disable query timeout by default.
    static constexpr uint64_t TIMEOUT_IN_MS = 0;
};

} // namespace common
} // namespace kuzu
