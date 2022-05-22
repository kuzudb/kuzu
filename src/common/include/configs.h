#pragma once

#include <cstdint>

namespace graphflow {
namespace common {

constexpr uint64_t DEFAULT_VECTOR_CAPACITY = 2048;

// Currently the system supports files with 2 different pages size, which we refer to as
// DEFAULT_PAGE_SIZE and LARGE_PAGE_SIZE. Default size of the page which is the unit of read/write
// to the database files, such as to store columns or lists. For now, this value cannot be changed.
// But technically it can change from 2^12 to 2^16. 2^12 lower bound is assuming the OS page size is
// 4K. 2^16 is because currently we leave 11 fixed number of bits for relOffInPage and the maximum
// number of bytes needed for an edge is 20 bytes so 11 + log_2(20) = 15.xxx, so certainly over
// 2^16-size pages, we cannot utilize the page for storing adjacency lists.
constexpr uint64_t DEFAULT_PAGE_SIZE_LOG_2 = 12;
constexpr uint64_t DEFAULT_PAGE_SIZE = 1 << DEFAULT_PAGE_SIZE_LOG_2;
// Page size for files with large pages, e.g., temporary files that are used by operators that may
// require large amounts of memory.
constexpr const uint64_t LARGE_PAGE_SIZE_LOG_2 = 18;
constexpr uint64_t LARGE_PAGE_SIZE = 1 << LARGE_PAGE_SIZE_LOG_2;

constexpr const double DEFAULT_HT_LOAD_FACTOR = 1.5;

constexpr const uint32_t VAR_LENGTH_EXTEND_MAX_DEPTH = 30;

// This is the default thread sleep time we use when a thread,
// e.g., a worker thread is in TaskScheduler, needs to block.
constexpr const uint64_t THREAD_SLEEP_TIME_WHEN_WAITING_IN_MICROS = 100;

// The number of pages for which we maintain a lock and a page version array for. Multi version file
// design is meant to not contain any page version arrays if a group of pages do not contain
// any updates to reduce our memory footprint.
constexpr const uint64_t MULTI_VERSION_FILE_PAGE_GROUP_SIZE = 64;

constexpr const uint64_t DEFAULT_CHECKPOINT_WAIT_TIMEOUT_FOR_TRANSACTIONS_TO_LEAVE_IN_MICROS =
    5000000;

struct StorageConfig {
    // The default amount of memory pre-allocated to both the default and large pages buffer pool.
    static constexpr uint64_t DEFAULT_BUFFER_POOL_SIZE = 1ull << 23; // (8MB)
    // The default ratio of buffer allocated to large pages.
    static constexpr double DEFAULT_PAGES_BUFFER_RATIO = 0.5;
    static constexpr double LARGE_PAGES_BUFFER_RATIO = 1.0 - DEFAULT_PAGES_BUFFER_RATIO;
    static constexpr char OVERFLOW_FILE_SUFFIX[] = ".ovf";
    static constexpr char COLUMN_FILE_SUFFIX[] = ".col";
    static constexpr char LISTS_FILE_SUFFIX[] = ".lists";
    static constexpr char WAL_FILE_SUFFIX[] = ".wal";
    static constexpr char INDEX_FILE_SUFFIX[] = ".hindex";
    // LIST_CHUNK_SIZE should strictly be a power of 2.
    constexpr static uint16_t LISTS_CHUNK_SIZE_LOG_2 = 9;
    constexpr static uint16_t LISTS_CHUNK_SIZE = 1 << LISTS_CHUNK_SIZE_LOG_2;
};

// Hash Index Configurations
struct HashIndexConfig {
    static constexpr uint64_t SLOT_CAPACITY = 4;
};

struct LoaderConfig {
    // Size (in bytes) of the chunks to be read in GraphLoader.
    static constexpr uint64_t CSV_READING_BLOCK_SIZE = 1 << 23;

    static constexpr char UNSTR_PROPERTY_SEPARATOR[] = ":";
    static constexpr char PROPERTY_DATATYPE_SEPARATOR[] = ":";
    static constexpr char DEFAULT_METADATA_JSON_FILENAME[] = "metadata.json";
    constexpr static char COMMENT_LINE_CHAR = '#';

    // mandatory fields in input CSV files
    static constexpr char ID_FIELD[] = "ID";
    static constexpr char START_ID_FIELD[] = "START_ID";
    static constexpr char END_ID_FIELD[] = "END_ID";
    static constexpr char START_ID_LABEL_FIELD[] = "START_ID_LABEL";
    static constexpr char END_ID_LABEL_FIELD[] = "END_ID_LABEL";
    static constexpr char LIST_FIELD_SUFFIX[] = "[]";

    // Default configuration for csv file parsing
    static constexpr char DEFAULT_ESCAPE_CHAR = '\\';
    static constexpr char DEFAULT_TOKEN_SEPARATOR = ',';
    static constexpr char DEFAULT_QUOTE_CHAR = '"';
    static constexpr char DEFAULT_LIST_BEGIN_CHAR = '[';
    static constexpr char DEFAULT_LIST_END_CHAR = ']';
};

} // namespace common
} // namespace graphflow
