#pragma once

#include "common/copier_config/rdf_reader_config.h"
#include "common/data_chunk/data_chunk.h"
#include "function/scalar_function.h"
#include "function/table_functions.h"
#include "function/table_functions/bind_data.h"
#include "function/table_functions/bind_input.h"
#include "function/table_functions/scan_functions.h"
#include "serd.h"

namespace kuzu {
namespace processor {

class RdfReader {
public:
    RdfReader(
        std::string filePath, common::FileType fileType, const common::RdfReaderConfig& config)
        : filePath{std::move(filePath)}, fileType{fileType}, mode{config.mode}, index{config.index},
          reader{nullptr}, rowOffset{0}, vectorSize{0}, status{SERD_SUCCESS}, sVector{nullptr},
          pVector{nullptr}, oVector{nullptr} {}

    ~RdfReader();

    inline void initReader() { initReader(prefixHandle, statementHandle); }
    inline void initCountReader() { initReader(nullptr, countStatementHandle); }

    common::offset_t read(common::DataChunk* dataChunkToRead);
    common::offset_t countLine();

private:
    static SerdStatus errorHandle(void* handle, const SerdError* error);
    static SerdStatus statementHandle(void* handle, SerdStatementFlags flags, const SerdNode* graph,
        const SerdNode* subject, const SerdNode* predicate, const SerdNode* object,
        const SerdNode* object_datatype, const SerdNode* object_lang);
    static SerdStatus prefixHandle(void* handle, const SerdNode* name, const SerdNode* uri);

    static SerdStatus countStatementHandle(void* handle, SerdStatementFlags flags,
        const SerdNode* graph, const SerdNode* subject, const SerdNode* predicate,
        const SerdNode* object, const SerdNode* object_datatype, const SerdNode* object_lang);

    void initReader(const SerdPrefixSink prefixSink_, const SerdStatementSink statementSink_);

private:
    const std::string filePath;
    common::FileType fileType;
    common::RdfReaderMode mode;
    storage::PrimaryKeyIndex* index;

    FILE* fp;
    SerdReader* reader;

    // TODO(Xiyang): use prefix to expand CURIE.
    const char* currentPrefix;
    common::offset_t rowOffset;
    common::offset_t vectorSize;
    SerdStatus status;

    common::ValueVector* sVector; // subject
    common::ValueVector* pVector; // predicate
    common::ValueVector* oVector; // object
};

struct RdfScanSharedState final : public function::ScanSharedState {
    std::unique_ptr<RdfReader> reader;

    RdfScanSharedState(common::ReaderConfig readerConfig, uint64_t numRows)
        : ScanSharedState{readerConfig, numRows} {
        initReader();
    }

    void read(common::DataChunk& dataChunk);

    void initReader();
};

struct RdfScan {
    static function::function_set getFunctionSet();

    static void tableFunc(function::TableFunctionInput& input, common::DataChunk& outputChunk);

    static std::unique_ptr<function::TableFuncBindData> bindFunc(main::ClientContext* /*context*/,
        function::TableFuncBindInput* input, catalog::Catalog* /*catalog*/,
        storage::StorageManager* /*storageManager*/);

    static std::unique_ptr<function::TableFuncSharedState> initSharedState(
        function::TableFunctionInitInput& input);

    static std::unique_ptr<function::TableFuncLocalState> initLocalState(
        function::TableFunctionInitInput& input, function::TableFuncSharedState* /*state*/,
        storage::MemoryManager* /*mm*/);
};

} // namespace processor
} // namespace kuzu
