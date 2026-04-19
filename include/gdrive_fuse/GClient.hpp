#pragma once

#include <nlohmann/json.hpp>

#include <atomic>
#include <condition_variable>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <thread>
#include <vector>

namespace gdrive_fuse {

class Auth;

/**
 * @brief Wrapper for Google Drive REST API
 *
 * This class provides a simplified interface to interact with the Google Drive API.
 * It handles request construction, authentication, and response parsing. Thread-safe.
 */
class GClient {
public:
    /**
     * @brief Construct a new GClient object
     *
     * @param auth Shared pointer to Auth object for authentication
     */
    explicit GClient(std::shared_ptr<Auth> auth);

    /**
     * @brief Destroy the GClient object
     */
    ~GClient();

    /**
     * @brief Structure representing a file or folder in Google Drive
     */
    struct FileInfo {
        std::string id;
        std::string name;
        std::string mimeType;
        size_t size;
        std::string modifiedTime;
        bool isFolder;
    };

    /**
     * @brief Directory listing result including ETag for conditional re-validation
     */
    struct DirListing {
        std::vector<FileInfo> files;
        std::string etag;  ///< ETag from the API response for conditional requests
    };

    /**
     * @brief List files in a directory
     *
     * @param parent_id Parent folder ID (use "root" for root directory)
     * @return DirListing files and ETag
     */
    DirListing listFiles(const std::string& parent_id = "root");

    /**
     * @brief Check whether a cached directory listing is still valid via ETag.
     *
     * Sends a conditional GET with If-None-Match. Returns std::nullopt when the
     * server responds with 304 (cache still valid). Returns a fresh DirListing
     * when the content has changed.
     *
     * @param parent_id  Parent folder ID
     * @param etag       ETag stored from the previous listFiles() call
     * @return std::nullopt if cache is still valid, fresh DirListing otherwise
     */
    std::optional<DirListing> revalidateDir(const std::string& parent_id, const std::string& etag);

    /**
     * @brief Get file metadata
     *
     * @param file_id File ID
     * @return FileInfo File information
     */
    FileInfo getFileMetadata(const std::string& file_id);

    /**
     * @brief Download file content
     *
     * @param file_id File ID
     * @return std::string File content
     */
    std::string downloadFile(const std::string& file_id);

    /**
     * @brief Upload a file
     *
     * @param name File name
     * @param content File content
     * @param parent_id Parent folder ID
     * @return std::string Uploaded file ID
     */
    std::string uploadFile(const std::string& name, const std::string& content,
                           const std::string& parent_id = "root");

    /**
     * @brief Delete a file
     *
     * @param file_id File ID
     * @return true if successful
     * @return false otherwise
     */
    bool deleteFile(const std::string& file_id);

    // -------------------------------------------------------------------------
    // Drive Changes API – background watcher
    // -------------------------------------------------------------------------

    /**
     * @brief Callback type invoked when remote changes are detected.
     *
     * The vector contains the Drive folder IDs whose contents changed.
     * An empty vector signals that ALL cached directories should be invalidated
     * (e.g. after a polling error).
     */
    using ChangeCallback = std::function<void(const std::vector<std::string>& changed_parent_ids)>;

    /**
     * @brief Start the background change-watcher thread.
     *
     * Fetches a startPageToken from the Drive Changes API and then polls
     * `changes.list` every @p interval_seconds seconds. On each poll the
     * @p callback is called with the affected parent folder IDs so the FUSE
     * cache can be invalidated precisely.
     *
     * Safe to call multiple times – a running watcher is stopped first.
     *
     * @param callback          Called from the watcher thread with changed IDs.
     * @param interval_seconds  Poll interval (default: 30 s).
     */
    void startChangeWatcher(ChangeCallback callback, int interval_seconds = 30);

    /**
     * @brief Stop the background change-watcher thread and block until it exits.
     */
    void stopChangeWatcher();

private:
    std::shared_ptr<Auth> auth_;
    mutable std::mutex mutex_;  // For thread-safety

    // Change watcher
    std::thread watcher_thread_;
    std::atomic<bool> watcher_running_{false};
    std::mutex watcher_cv_mutex_;
    std::condition_variable watcher_cv_;

    /// Single poll of changes.list; returns {changed_parent_ids, new_page_token}.
    /// Returns empty parent list + empty token on error.
    struct PollResult {
        std::vector<std::string> changed_parent_ids;
        std::string next_page_token;
    };
    std::string getStartPageToken();
    PollResult pollChanges(const std::string& page_token);
    void watcherLoop(ChangeCallback callback, int interval_seconds);

    static const std::string API_BASE_URL;

    /// Full request; fills *etag_out with the response ETag when non-null.
    nlohmann::json makeRequest(const std::string& endpoint, const std::string& method = "GET",
                               const nlohmann::json& body       = nullptr,
                               std::string* etag_out            = nullptr,
                               const std::string& if_none_match = "");

    /// Parse a raw JSON files-list response into a vector of FileInfo.
    static std::vector<FileInfo> parseFileList(const nlohmann::json& json);
};

}  // namespace gdrive_fuse
