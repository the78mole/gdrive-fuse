#include "gdrive_fuse/FuseOps.hpp"
#include "gdrive_fuse/GClient.hpp"
#include <spdlog/spdlog.h>
#include <cstring>
#include <algorithm>

namespace gdrive_fuse {

FuseOps* FuseOps::instance_ = nullptr;

FuseOps::FuseOps(std::shared_ptr<GClient> client)
    : client_(client) {
    spdlog::debug("FuseOps object created");
}

FuseOps::~FuseOps() {
    spdlog::debug("FuseOps object destroyed");
}

void FuseOps::setInstance(FuseOps* instance) {
    instance_ = instance;
}

fuse_operations FuseOps::getFuseOperations() {
    fuse_operations ops = {};
    ops.getattr = FuseOps::getattr;
    ops.readdir = FuseOps::readdir;
    ops.read = FuseOps::read;
    return ops;
}

int FuseOps::getattr(const char* path, struct stat* stbuf, struct fuse_file_info* fi) {
    (void) fi;  // Unused parameter
    
    if (!instance_) {
        return -ENOENT;
    }
    
    std::lock_guard<std::mutex> lock(instance_->mutex_);
    
    memset(stbuf, 0, sizeof(struct stat));
    
    std::string path_str(path);
    spdlog::debug("getattr called for path: {}", path_str);
    
    // Root directory
    if (path_str == "/") {
        stbuf->st_mode = S_IFDIR | 0755;
        stbuf->st_nlink = 2;
        return 0;
    }
    
    try {
        // Get file ID from path
        std::string file_id = instance_->getFileIdFromPath(path_str);
        if (file_id.empty()) {
            return -ENOENT;
        }
        
        // Get file metadata
        auto info = instance_->client_->getFileMetadata(file_id);
        
        if (info.isFolder) {
            stbuf->st_mode = S_IFDIR | 0755;
            stbuf->st_nlink = 2;
        } else {
            stbuf->st_mode = S_IFREG | 0644;
            stbuf->st_nlink = 1;
            stbuf->st_size = info.size;
        }
        
        return 0;
    } catch (const std::exception& e) {
        spdlog::error("getattr error: {}", e.what());
        return -ENOENT;
    }
}

int FuseOps::readdir(const char* path, void* buf, fuse_fill_dir_t filler,
                    off_t offset, struct fuse_file_info* fi,
                    enum fuse_readdir_flags flags) {
    (void) offset;  // Unused parameter
    (void) fi;      // Unused parameter
    (void) flags;   // Unused parameter
    
    if (!instance_) {
        return -ENOENT;
    }
    
    std::lock_guard<std::mutex> lock(instance_->mutex_);
    
    std::string path_str(path);
    spdlog::debug("readdir called for path: {}", path_str);
    
    // Add standard entries
    filler(buf, ".", nullptr, 0, static_cast<fuse_fill_dir_flags>(0));
    filler(buf, "..", nullptr, 0, static_cast<fuse_fill_dir_flags>(0));
    
    try {
        std::string parent_id = "root";
        
        // If not root, get the folder ID
        if (path_str != "/") {
            parent_id = instance_->getFileIdFromPath(path_str);
            if (parent_id.empty()) {
                return -ENOENT;
            }
        }
        
        // List files in the directory
        auto files = instance_->client_->listFiles(parent_id);
        
        for (const auto& file : files) {
            // Cache the path to ID mapping
            std::string file_path = path_str;
            if (file_path != "/") {
                file_path += "/";
            }
            file_path += file.name;
            instance_->path_to_id_cache_[file_path] = file.id;
            
            filler(buf, file.name.c_str(), nullptr, 0, static_cast<fuse_fill_dir_flags>(0));
        }
        
        return 0;
    } catch (const std::exception& e) {
        spdlog::error("readdir error: {}", e.what());
        return -ENOENT;
    }
}

int FuseOps::read(const char* path, char* buf, size_t size, off_t offset,
                 struct fuse_file_info* fi) {
    (void) fi;  // Unused parameter
    
    if (!instance_) {
        return -ENOENT;
    }
    
    std::lock_guard<std::mutex> lock(instance_->mutex_);
    
    std::string path_str(path);
    spdlog::debug("read called for path: {}, size: {}, offset: {}", path_str, size, offset);
    
    try {
        // Get file ID from path
        std::string file_id = instance_->getFileIdFromPath(path_str);
        if (file_id.empty()) {
            return -ENOENT;
        }
        
        // Download file content
        // Note: This is a simplified implementation that downloads the entire file.
        // A production implementation should use range requests or implement a caching layer
        // to handle large files efficiently and support partial reads without full downloads.
        std::string content = instance_->client_->downloadFile(file_id);
        
        if (content.empty()) {
            return 0;
        }
        
        size_t len = content.size();
        
        if (offset >= static_cast<off_t>(len)) {
            return 0;
        }
        
        size_t bytes_to_read = std::min(size, len - offset);
        memcpy(buf, content.c_str() + offset, bytes_to_read);
        
        return bytes_to_read;
    } catch (const std::exception& e) {
        spdlog::error("read error: {}", e.what());
        return -EIO;
    }
}

std::string FuseOps::getFileIdFromPath(const std::string& path) {
    // Check cache first
    auto it = path_to_id_cache_.find(path);
    if (it != path_to_id_cache_.end()) {
        return it->second;
    }
    
    // If not in cache, need to traverse from root
    // This is a simplified implementation
    // In production, you'd want a more sophisticated caching mechanism
    
    if (path == "/") {
        return "root";
    }
    
    // Split path into components
    std::vector<std::string> components;
    std::string current;
    for (char c : path) {
        if (c == '/') {
            if (!current.empty()) {
                components.push_back(current);
                current.clear();
            }
        } else {
            current += c;
        }
    }
    if (!current.empty()) {
        components.push_back(current);
    }
    
    // Traverse from root
    std::string current_id = "root";
    std::string current_path = "";
    
    for (const auto& component : components) {
        current_path += "/" + component;
        
        // Check cache
        auto cached = path_to_id_cache_.find(current_path);
        if (cached != path_to_id_cache_.end()) {
            current_id = cached->second;
            continue;
        }
        
        // List files in current directory
        auto files = client_->listFiles(current_id);
        
        bool found = false;
        for (const auto& file : files) {
            if (file.name == component) {
                current_id = file.id;
                path_to_id_cache_[current_path] = file.id;
                found = true;
                break;
            }
        }
        
        if (!found) {
            return "";
        }
    }
    
    return current_id;
}

} // namespace gdrive_fuse
