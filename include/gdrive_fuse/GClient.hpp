#pragma once

#include <string>
#include <memory>
#include <mutex>
#include <vector>
#include <nlohmann/json.hpp>

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
     * @brief List files in a directory
     * 
     * @param parent_id Parent folder ID (use "root" for root directory)
     * @return std::vector<FileInfo> List of files
     */
    std::vector<FileInfo> listFiles(const std::string& parent_id = "root");

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

private:
    std::shared_ptr<Auth> auth_;
    mutable std::mutex mutex_;  // For thread-safety
    
    static const std::string API_BASE_URL;
    
    nlohmann::json makeRequest(const std::string& endpoint, const std::string& method = "GET",
                               const nlohmann::json& body = nullptr);
};

} // namespace gdrive_fuse
