#include "main/vector_extension.h"

#include "catalog/hnsw_index_catalog_entry.h"
#include "function/hnsw_index_functions.h"
#include "main/client_context.h"
#include "main/database.h"
#include "storage/storage_manager.h"
#include "transaction/transaction_manager.h"

#include <atomic>
#include <thread>
#include <vector>
#include <mutex>

namespace kuzu {
namespace vector_extension {

static void initHNSWEntries(main::ClientContext* context, transaction::Transaction* txn) {
    auto storageManager = storage::StorageManager::Get(*context);
    auto catalog = catalog::Catalog::Get(*context);
    auto* database = context->getDatabase();

    // Collect HNSW indexes
    std::vector<catalog::IndexCatalogEntry*> hnswIndexes;
    for (auto& indexEntry : catalog->getIndexEntries(txn)) {
        // Cancellation check during collection
        if (database->vectorIndexLoadCancelled.load(std::memory_order_acquire)) {
            return;
        }

        if (indexEntry->getIndexType() == HNSWIndexCatalogEntry::TYPE_NAME &&
            !indexEntry->isLoaded()) {
            hnswIndexes.push_back(indexEntry);
        }
    }

    if (hnswIndexes.empty()) {
        return;
    }

    // Parallel loading with thread pool
    size_t numThreads = std::min(
        static_cast<size_t>(context->getDatabase()->getConfig().maxNumThreads),
        hnswIndexes.size()
    );

    std::atomic<size_t> nextIndexToProcess{0};
    std::vector<std::thread> workers;
    std::mutex errorMutex;
    std::vector<std::string> errors;

    // Create fixed number of worker threads
    for (size_t i = 0; i < numThreads; ++i) {
        workers.emplace_back([&, database]() {
            while (true) {
                // Cancellation check at loop start
                if (database->vectorIndexLoadCancelled.load(std::memory_order_acquire)) {
                    break;
                }

                size_t idx = nextIndexToProcess.fetch_add(1);
                if (idx >= hnswIndexes.size()) {
                    break;
                }

                auto* indexEntry = hnswIndexes[idx];
                try {
                    // Cancellation check before loading
                    if (database->vectorIndexLoadCancelled.load(std::memory_order_acquire)) {
                        break;
                    }

                    // Deserialize aux info
                    indexEntry->setAuxInfo(
                        HNSWIndexAuxInfo::deserialize(indexEntry->getAuxBufferReader())
                    );

                    // Load index in storage
                    auto& nodeTable = storageManager->getTable(indexEntry->getTableID())
                        ->cast<storage::NodeTable>();
                    auto optionalIndex = nodeTable.getIndexHolder(indexEntry->getIndexName());

                    if (optionalIndex.has_value()) {
                        auto& indexHolder = optionalIndex.value().get();
                        if (!indexHolder.isLoaded()) {
                            // Cancellation check before expensive loading
                            if (database->vectorIndexLoadCancelled.load(std::memory_order_acquire)) {
                                break;
                            }

                            indexHolder.load(context, storageManager, indexEntry);
                        }
                    }

                } catch (const std::exception& e) {
                    std::lock_guard<std::mutex> lock(errorMutex);
                    errors.push_back(indexEntry->getIndexName() + ": " + e.what());
                }
            }
        });
    }

    // Wait for all threads
    for (auto& worker : workers) {
        worker.join();
    }

    // Handle errors only if not cancelled
    if (!database->vectorIndexLoadCancelled.load(std::memory_order_acquire) && !errors.empty()) {
        std::string errorMsg = "HNSW index loading failed:\n";
        for (const auto& error : errors) {
            errorMsg += "  - " + error + "\n";
        }
        throw common::RuntimeException(errorMsg);
    }
}

// Synchronous HNSW index loading function (used during recovery and by background thread)
static void loadHNSWIndexesSync(main::Database* database,
    std::shared_ptr<common::DatabaseLifeCycleManager> lifeCycleManager) {
    try {
        // CRITICAL SECTION: Check and create ClientContext atomically
        // This prevents TOCTOU race with destructor
        main::ClientContext* bgContextPtr = nullptr;
        {
            std::lock_guard<std::mutex> lock(database->backgroundThreadStartMutex);

            // Check if Database already closed
            if (lifeCycleManager->isDatabaseClosed) {
                return;
            }

            // Create ClientContext while holding lock
            bgContextPtr = new main::ClientContext(database);
        }
        // Lock released: Destructor can now proceed if needed

        // Wrap in unique_ptr for automatic cleanup
        std::unique_ptr<main::ClientContext> bgContext(bgContextPtr);

        // Early exit if cancelled (for background thread scenario)
        if (database->vectorIndexLoadCancelled.load(std::memory_order_acquire)) {
            return;
        }

        // Begin READ_ONLY transaction
        auto* txn = database->getTransactionManager()->beginTransaction(
            *bgContext,
            transaction::TransactionType::READ_ONLY
        );

        // Early exit if cancelled
        if (database->vectorIndexLoadCancelled.load(std::memory_order_acquire)) {
            database->getTransactionManager()->rollback(*bgContext, txn);
            return;
        }

        // Execute HNSW loading
        initHNSWEntries(bgContext.get(), txn);

        // Check cancellation before committing
        if (database->vectorIndexLoadCancelled.load(std::memory_order_acquire)) {
            database->getTransactionManager()->rollback(*bgContext, txn);
            return;
        }

        // Commit transaction
        database->getTransactionManager()->commit(*bgContext, txn);

        // Notify completion (internally checks vectorIndexLoadCancelled)
        database->notifyVectorIndexLoadComplete(true);

    } catch (const std::exception& e) {
        // Notify error (internally checks vectorIndexLoadCancelled)
        database->notifyVectorIndexLoadComplete(false, e.what());

    } catch (...) {
        // Notify error (internally checks vectorIndexLoadCancelled)
        database->notifyVectorIndexLoadComplete(false, "Unknown error");
    }
}

void VectorExtension::load(main::ClientContext* context) {
    auto& db = *context->getDatabase();

    // Register vector extension functions
    extension::ExtensionUtils::addTableFunc<QueryVectorIndexFunction>(db);
    extension::ExtensionUtils::addInternalStandaloneTableFunc<InternalCreateHNSWIndexFunction>(db);
    extension::ExtensionUtils::addInternalStandaloneTableFunc<InternalFinalizeHNSWIndexFunction>(db);
    extension::ExtensionUtils::addStandaloneTableFunc<CreateVectorIndexFunction>(db);
    extension::ExtensionUtils::addInternalStandaloneTableFunc<InternalDropHNSWIndexFunction>(db);
    extension::ExtensionUtils::addStandaloneTableFunc<DropVectorIndexFunction>(db);
    extension::ExtensionUtils::registerIndexType(db, OnDiskHNSWIndex::getIndexType());

    // Capture Database* and shared_ptr to lifecycle manager
    auto* database = context->getDatabase();
    auto lifeCycleManager = database->dbLifeCycleManager;

    // Check if we are in recovery mode (WAL replay)
    // During recovery, we must load indexes synchronously to avoid race conditions where
    // WAL records (e.g., NodeDeletionRecord) access indexes before background loading completes
    if (lifeCycleManager->isRecoveryInProgress.load(std::memory_order_acquire)) {
        // Synchronous loading during recovery
        loadHNSWIndexesSync(database, lifeCycleManager);
        return;
    }

    // Check if extension is statically linked (test environment)
    // Static-linked extensions should load synchronously for testing reliability
#if defined(__STATIC_LINK_EXTENSION_TEST__) || !defined(BUILD_DYNAMIC_LOAD)
    bool isStaticLinked = true;
#else
    bool isStaticLinked = false;
#endif

    if (isStaticLinked) {
        // Synchronous loading for static-linked extensions (tests)
        // This ensures indexes are immediately ready for use after Database construction
        loadHNSWIndexesSync(database, lifeCycleManager);
        return;
    }

    // Normal operation (dynamic extension): start background loading thread
    // This allows the database to become available immediately while indexes load in background
    std::thread loaderThread([database, lifeCycleManager]() {
        loadHNSWIndexesSync(database, lifeCycleManager);
    });

    database->startVectorIndexLoader(std::move(loaderThread));
}

} // namespace vector_extension
} // namespace kuzu

#if defined(BUILD_DYNAMIC_LOAD)
extern "C" {
// Because we link against the static library on windows, we implicitly inherit KUZU_STATIC_DEFINE,
// which cancels out any exporting, so we can't use KUZU_API.
#if defined(_WIN32)
#define INIT_EXPORT __declspec(dllexport)
#else
#define INIT_EXPORT __attribute__((visibility("default")))
#endif
INIT_EXPORT void init(kuzu::main::ClientContext* context) {
    kuzu::vector_extension::VectorExtension::load(context);
}

INIT_EXPORT const char* name() {
    return kuzu::vector_extension::VectorExtension::EXTENSION_NAME;
}
}
#endif
