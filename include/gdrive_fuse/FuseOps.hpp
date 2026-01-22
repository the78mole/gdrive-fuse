#pragma once

#include <memory>
#include <mutex>
#include <unordered_map>
#include <string>

#define FUSE_USE_VERSION 31
#include <fuse3/fuse.h>

namespace gdrive_fuse {

class GClient;

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
    static int readdir(const char* path, void* buf, fuse_fill_dir_t filler,
                      off_t offset, struct fuse_file_info* fi,
                      enum fuse_readdir_flags flags);

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

private:
    std::shared_ptr<GClient> client_;
    mutable std::mutex mutex_;  // For thread-safety
    
    // Cache for path to file ID mapping
    std::unordered_map<std::string, std::string> path_to_id_cache_;
    
    static FuseOps* instance_;  // Singleton instance for callbacks
    
    std::string getFileIdFromPath(const std::string& path);
};

} // namespace gdrive_fuse
