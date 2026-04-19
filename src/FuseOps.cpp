#include "gdrive_fuse/FuseOps.hpp"

#include "gdrive_fuse/GClient.hpp"

#include <spdlog/spdlog.h>

#include <algorithm>
#include <cstring>

namespace {

/// Returns true for Google Workspace MIME types that have no binary blob.
bool isWorkspaceMimeType(const std::string& mime) {
    return mime.rfind("application/vnd.google-apps.", 0) == 0 &&
           mime != "application/vnd.google-apps.folder";
}

/// Returns the browser-edit URL for a Google Workspace file.
std::string workspaceUrl(const std::string& mime, const std::string& file_id) {
    if (mime == "application/vnd.google-apps.document")
        return "https://docs.google.com/document/d/" + file_id + "/edit";
    if (mime == "application/vnd.google-apps.spreadsheet")
        return "https://docs.google.com/spreadsheets/d/" + file_id + "/edit";
    if (mime == "application/vnd.google-apps.presentation")
        return "https://docs.google.com/presentation/d/" + file_id + "/edit";
    if (mime == "application/vnd.google-apps.form")
        return "https://docs.google.com/forms/d/" + file_id + "/edit";
    if (mime == "application/vnd.google-apps.drawing")
        return "https://docs.google.com/drawings/d/" + file_id + "/edit";
    return "https://drive.google.com/open?id=" + file_id;
}

/// Generates a .desktop file that opens the Workspace document in a browser.
std::string makeDesktopContent(const std::string& name, const std::string& mime,
                               const std::string& file_id) {
    return "[Desktop Entry]\nType=Link\nName=" + name + "\nURL=" + workspaceUrl(mime, file_id) +
           "\nIcon=text-html\n";
}

/// Returns the filename as it should appear in the directory listing.
/// Workspace files get a .desktop suffix so the file manager opens them correctly.
std::string displayFilename(const std::string& name, const std::string& mime) {
    if (isWorkspaceMimeType(mime))
        return name + ".desktop";
    return name;
}

}  // anonymous namespace

namespace gdrive_fuse {

FuseOps* FuseOps::instance_ = nullptr;

FuseOps::FuseOps(std::shared_ptr<GClient> client) : client_(client) {
    spdlog::debug("FuseOps object created");

    // Register background change watcher – invalidates cached dir listings
    // whenever Google Drive reports remote changes.
    client_->startChangeWatcher(
        [this](const std::vector<std::string>& changed_ids) {
            std::lock_guard<std::mutex> lock(mutex_);
            for (const auto& id : changed_ids)
                invalidateDirCache(id);
        },
        30  // poll interval in seconds
    );
}

FuseOps::~FuseOps() {
    client_->stopChangeWatcher();
    spdlog::debug("FuseOps object destroyed");
}

void FuseOps::setInstance(FuseOps* instance) {
    instance_ = instance;
}

fuse_operations FuseOps::getFuseOperations() {
    fuse_operations ops = {};
    ops.getattr         = FuseOps::getattr;
    ops.readdir         = FuseOps::readdir;
    ops.read            = FuseOps::read;
    return ops;
}

int FuseOps::getattr(const char* path, struct stat* stbuf, struct fuse_file_info* fi) {
    (void)fi;  // Unused parameter

    if (!instance_) {
        return -ENOENT;
    }

    std::lock_guard<std::mutex> lock(instance_->mutex_);

    memset(stbuf, 0, sizeof(struct stat));

    std::string path_str(path);
    spdlog::debug("getattr called for path: {}", path_str);

    // Root directory
    if (path_str == "/") {
        stbuf->st_mode  = S_IFDIR | 0755;
        stbuf->st_nlink = 2;
        return 0;
    }

    try {
        // Get file ID from path
        std::string file_id = instance_->getFileIdFromPath(path_str);
        if (file_id.empty()) {
            return -ENOENT;
        }

        // Check metadata cache before making an API call
        GClient::FileInfo info;
        auto cache_it = instance_->metadata_cache_.find(file_id);
        if (cache_it != instance_->metadata_cache_.end()) {
            info = cache_it->second;
        } else {
            info                                = instance_->client_->getFileMetadata(file_id);
            instance_->metadata_cache_[file_id] = info;
        }

        if (info.isFolder) {
            stbuf->st_mode  = S_IFDIR | 0755;
            stbuf->st_nlink = 2;
        } else if (isWorkspaceMimeType(info.mimeType)) {
            // Workspace files are presented as .desktop links – no binary blob.
            std::string content = makeDesktopContent(info.name, info.mimeType, info.id);
            stbuf->st_mode      = S_IFREG | 0644;
            stbuf->st_nlink     = 1;
            stbuf->st_size      = static_cast<off_t>(content.size());
        } else {
            stbuf->st_mode  = S_IFREG | 0644;
            stbuf->st_nlink = 1;
            stbuf->st_size  = info.size;
        }

        return 0;
    } catch (const std::exception& e) {
        spdlog::error("getattr error: {}", e.what());
        return -ENOENT;
    }
}

int FuseOps::readdir(const char* path, void* buf, fuse_fill_dir_t filler, off_t offset,
                     struct fuse_file_info* fi, enum fuse_readdir_flags flags) {
    (void)offset;  // Unused parameter
    (void)fi;      // Unused parameter
    (void)flags;   // Unused parameter

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
        if (path_str != "/") {
            parent_id = instance_->getFileIdFromPath(path_str);
            if (parent_id.empty()) {
                return -ENOENT;
            }
        }

        auto& entry = instance_->dir_cache_[parent_id];
        auto now    = std::chrono::steady_clock::now();

        if (entry.state == DirCacheState::FRESH) {
            auto age =
                std::chrono::duration_cast<std::chrono::seconds>(now - entry.last_fetched).count();
            if (age >= DIR_CACHE_TTL_SECONDS) {
                entry.state = DirCacheState::STALE;
            }
        }

        if (entry.state == DirCacheState::INVALID) {
            // Cold cache – full fetch
            auto listing       = instance_->client_->listFiles(parent_id);
            entry.files        = std::move(listing.files);
            entry.etag         = std::move(listing.etag);
            entry.last_fetched = now;
            entry.state        = DirCacheState::FRESH;
            spdlog::debug("readdir '{}': cold fetch", path_str);
        } else if (entry.state == DirCacheState::STALE) {
            // TTL expired – cheap ETag revalidation
            auto fresh = instance_->client_->revalidateDir(parent_id, entry.etag);
            if (fresh) {
                // Content changed
                entry.files = std::move(fresh->files);
                entry.etag  = std::move(fresh->etag);
                spdlog::debug("readdir '{}': ETag mismatch, cache updated", path_str);
            } else {
                spdlog::debug("readdir '{}': ETag match, cache still valid", path_str);
            }
            entry.last_fetched = now;
            entry.state        = DirCacheState::FRESH;
        }
        // else DirCacheState::FRESH – serve from cache, no API call

        instance_->populateCaches(path_str, entry.files);

        for (const auto& file : entry.files) {
            std::string dname = displayFilename(file.name, file.mimeType);
            filler(buf, dname.c_str(), nullptr, 0, static_cast<fuse_fill_dir_flags>(0));
        }

        return 0;
    } catch (const std::exception& e) {
        spdlog::error("readdir error: {}", e.what());
        return -ENOENT;
    }
}

void FuseOps::populateCaches(const std::string& path_str,
                             const std::vector<GClient::FileInfo>& files) {
    for (const auto& file : files) {
        std::string file_path = path_str;
        if (file_path != "/")
            file_path += "/";
        file_path += displayFilename(file.name, file.mimeType);
        path_to_id_cache_[file_path] = file.id;
        metadata_cache_[file.id]     = file;
    }
}

void FuseOps::invalidateDirCache(const std::string& parent_id) {
    if (parent_id.empty()) {
        for (auto& [id, entry] : dir_cache_) {
            entry.state = DirCacheState::INVALID;
        }
        spdlog::debug("All directory caches invalidated");
    } else {
        auto it = dir_cache_.find(parent_id);
        if (it != dir_cache_.end()) {
            it->second.state = DirCacheState::INVALID;
        }
        spdlog::debug("Directory cache invalidated for '{}'", parent_id);
    }
}

int FuseOps::read(const char* path, char* buf, size_t size, off_t offset,
                  struct fuse_file_info* fi) {
    (void)fi;  // Unused parameter

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

        // For Google Workspace files (Docs, Sheets, Slides …) return a .desktop
        // stub that opens the document in the browser instead of downloading.
        std::string content;
        auto meta_it = instance_->metadata_cache_.find(file_id);
        if (meta_it != instance_->metadata_cache_.end() &&
            isWorkspaceMimeType(meta_it->second.mimeType)) {
            content = makeDesktopContent(meta_it->second.name, meta_it->second.mimeType, file_id);
        } else {
            content = instance_->client_->downloadFile(file_id);
        }

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
    std::string current_id   = "root";
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
        auto listing = client_->listFiles(current_id);

        bool found = false;
        for (const auto& file : listing.files) {
            path_to_id_cache_["/" + component] = file.id;
            metadata_cache_[file.id]           = file;
            if (file.name == component) {
                current_id                      = file.id;
                path_to_id_cache_[current_path] = file.id;
                found                           = true;
            }
        }

        if (!found) {
            return "";
        }
    }

    return current_id;
}

}  // namespace gdrive_fuse
