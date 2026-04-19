#pragma once

#include "gdrive_fuse/GClient.hpp"

#include <chrono>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

#define FUSE_USE_VERSION 31
#include <fuse3/fuse.h>

namespace gdrive_fuse {

/**
 * @brief FUSE operations implementation
 *
 * This class implements the basic FUSE callbacks for filesystem operations.
 * Thread-safe design to handle concurrent FUSE operations.
 */
class FuseOps {
public:
    /**
     * @brief Construct a new FuseOps object
     *
     * @param client Shared pointer to GClient for Drive API access
     */
    explicit FuseOps(std::shared_ptr<GClient> client);

    /**
     * @brief Destroy the FuseOps object
     */
    ~FuseOps();

    /**
     * @brief Get FUSE operations structure
     *
     * @return fuse_operations The operations structure
     */
    static fuse_operations getFuseOperations();

    /**
     * @brief Get file attributes (stat)
     *
     * @param path File path
     * @param stbuf Stat buffer to fill
     * @param fi File info (can be nullptr)
     * @return int 0 on success, negative error code on failure
     */
    static int getattr(const char* path, struct stat* stbuf, struct fuse_file_info* fi);

    /**
     * @brief Read directory contents
     *
     * @param path Directory path
     * @param buf Buffer for directory entries
     * @param filler Function to add entries to buffer
     * @param offset Offset for pagination
     * @param fi File info
     * @param flags FUSE readdir flags
     * @return int 0 on success, negative error code on failure
     */
    static int readdir(const char* path, void* buf, fuse_fill_dir_t filler, off_t offset,
                       struct fuse_file_info* fi, enum fuse_readdir_flags flags);

    /**
     * @brief Read file content
     *
     * @param path File path
     * @param buf Buffer to fill
     * @param size Number of bytes to read
     * @param offset Offset in file
     * @param fi File info
     * @return int Number of bytes read on success, negative error code on failure
     */
    static int read(const char* path, char* buf, size_t size, off_t offset,
                    struct fuse_file_info* fi);

    /**
     * @brief Set instance for static callbacks
     *
     * @param instance Pointer to FuseOps instance
     */
    static void setInstance(FuseOps* instance);

    /**
     * @brief Invalidate the cached directory listing for a given Drive folder ID.
     *
     * Must be called after any write operation (upload, delete, rename) on that
     * directory so the next readdir triggers a fresh fetch instead of serving
     * stale data.
     *
     * @param parent_id  Drive folder ID whose cache entry should be invalidated.
     *                   Pass an empty string to invalidate all cached directories.
     */
    void invalidateDirCache(const std::string& parent_id = "");

private:
    std::shared_ptr<GClient> client_;
    mutable std::mutex mutex_;  // For thread-safety

    // Cache for path to file ID mapping
    std::unordered_map<std::string, std::string> path_to_id_cache_;

    // Cache for file metadata (file_id -> FileInfo)
    std::unordered_map<std::string, GClient::FileInfo> metadata_cache_;

    // Cache for directory listings (parent_id -> entry)
    // State machine:
    //   FRESH    – TTL not expired, serve directly from cache
    //   STALE    – TTL expired, must revalidate via ETag before serving
    //   INVALID  – no data yet or after a write operation
    enum class DirCacheState { FRESH, STALE, INVALID };

    struct DirCacheEntry {
        std::vector<GClient::FileInfo> files;
        std::string etag;
        std::chrono::steady_clock::time_point last_fetched;
        DirCacheState state = DirCacheState::INVALID;
    };
    std::unordered_map<std::string, DirCacheEntry> dir_cache_;

    /// Time after which a FRESH entry becomes STALE and triggers an ETag revalidation.
    static constexpr int DIR_CACHE_TTL_SECONDS = 30;

    static FuseOps* instance_;  // Singleton instance for callbacks

    std::string getFileIdFromPath(const std::string& path);

    /// Populate path_to_id_cache_ and metadata_cache_ from a directory listing.
    void populateCaches(const std::string& path_str, const std::vector<GClient::FileInfo>& files);
};

}  // namespace gdrive_fuse
