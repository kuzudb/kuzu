#pragma once

#include <cstdint>
#include <string>
#include <unordered_map>

namespace kuzu {
namespace common {

constexpr uint64_t DEFAULT_VECTOR_CAPACITY_LOG_2 = 11;
constexpr uint64_t DEFAULT_VECTOR_CAPACITY = (uint64_t)1 << DEFAULT_VECTOR_CAPACITY_LOG_2;

constexpr const double DEFAULT_HT_LOAD_FACTOR = 1.5;
constexpr const uint32_t VAR_LENGTH_EXTEND_MAX_DEPTH = 30;

// This is the default thread sleep time we use when a thread,
// e.g., a worker thread is in TaskScheduler, needs to block.
constexpr const uint64_t THREAD_SLEEP_TIME_WHEN_WAITING_IN_MICROS = 500;

// The number of pages for which we maintain a lock and a page version array for. Multi version file
// design is meant to not contain any page version arrays if a group of pages do not contain
// any updates to reduce our memory footprint.
constexpr const uint64_t MULTI_VERSION_FILE_PAGE_GROUP_SIZE = 64;
constexpr const uint64_t DEFAULT_CHECKPOINT_WAIT_TIMEOUT_FOR_TRANSACTIONS_TO_LEAVE_IN_MICROS =
    5000000;

const std::string INTERNAL_ID_SUFFIX = "_id";

enum PageSizeClass : uint8_t {
    PAGE_4KB = 12,
    PAGE_256KB = 18,
};

// Currently the system supports files with 2 different pages size, which we refer to as
// DEFAULT_PAGE_SIZE and LARGE_PAGE_SIZE. Default size of the page which is the unit of read/write
// to the database files, such as to store columns or lists. For now, this value cannot be changed.
// But technically it can change from 2^12 to 2^16. 2^12 lower bound is assuming the OS page size is
// 4K. 2^16 is because currently we leave 11 fixed number of bits for relOffInPage and the maximum
// number of bytes needed for an edge is 20 bytes so 11 + log_2(20) = 15.xxx, so certainly over
// 2^16-size pages, we cannot utilize the page for storing adjacency lists.
struct BufferPoolConstants {
    static constexpr uint64_t DEFAULT_PAGE_SIZE = (std::uint64_t)1 << PageSizeClass::PAGE_4KB;
    // Page size for files with large pages, e.g., temporary files that are used by operators that
    // may require large amounts of memory.
    static constexpr uint64_t LARGE_PAGE_SIZE = (std::uint64_t)1 << PageSizeClass::PAGE_256KB;
    static constexpr double DEFAULT_BUFFER_POOL_RATIO = 0.8;

    // All supported page sizes.
    static constexpr PageSizeClass PAGE_SIZE_CLASSES[] = {
        PageSizeClass::PAGE_4KB, PageSizeClass::PAGE_256KB};

    static constexpr uint64_t PURGE_EVICTION_QUEUE_INTERVAL = 1024;
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
    constexpr static double ARRAY_RESIZING_FACTOR = 1.2;
};

struct ListsMetadataConstants {
    // LIST_CHUNK_SIZE should strictly be a power of 2.
    constexpr static uint16_t LISTS_CHUNK_SIZE_LOG_2 = 9;
    constexpr static uint16_t LISTS_CHUNK_SIZE = 1 << LISTS_CHUNK_SIZE_LOG_2;
    // All pageLists (defined later) are broken in pieces (called a pageListGroups) of size
    // PAGE_LIST_GROUP_SIZE + 1 each and chained together. In each piece, there are
    // PAGE_LIST_GROUP_SIZE elements of that list and the offset to the next pageListGroups in the
    // blob that contains all pageListGroups of all lists.
    static constexpr uint32_t PAGE_LIST_GROUP_SIZE = 3;
    static constexpr uint32_t PAGE_LIST_GROUP_WITH_NEXT_PTR_SIZE = PAGE_LIST_GROUP_SIZE + 1;
};

// Hash Index Configurations
struct HashIndexConstants {
    static constexpr uint8_t SLOT_CAPACITY_LOG_2 = 2;
    static constexpr uint8_t SLOT_CAPACITY = (uint64_t)1 << SLOT_CAPACITY_LOG_2;
};

struct CopyConstants {
    // Size (in bytes) of the chunks to be read in Node/Rel Copier
    static constexpr uint64_t CSV_READING_BLOCK_SIZE = 1 << 23;

    // Number of tasks to be assigned in a batch when reading files.
    static constexpr uint64_t NUM_COPIER_TASKS_TO_SCHEDULE_PER_BATCH = 200;

    // Lower bound for number of incomplete tasks in copier to trigger scheduling a new batch.
    static constexpr uint64_t MINIMUM_NUM_COPIER_TASKS_TO_SCHEDULE_MORE = 50;

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

struct EnumeratorKnobs {
    static constexpr double PREDICATE_SELECTIVITY = 0.1;
    static constexpr double FLAT_PROBE_PENALTY = 10;
};

} // namespace common
} // namespace kuzu
