#include "gdrive_fuse/GClient.hpp"

#include "gdrive_fuse/Auth.hpp"

#include <cpr/cpr.h>
#include <spdlog/spdlog.h>

#include <optional>
#include <unordered_set>

namespace gdrive_fuse {

const std::string GClient::API_BASE_URL = "https://www.googleapis.com/drive/v3";

GClient::GClient(std::shared_ptr<Auth> auth) : auth_(auth) {
    spdlog::debug("GClient object created");
}

GClient::~GClient() {
    stopChangeWatcher();
    spdlog::debug("GClient object destroyed");
}

// static helper
std::vector<GClient::FileInfo> GClient::parseFileList(const nlohmann::json& json) {
    std::vector<FileInfo> files;
    if (!json.contains("files"))
        return files;
    for (const auto& file : json["files"]) {
        FileInfo info;
        info.id           = file.value("id", "");
        info.name         = file.value("name", "");
        info.mimeType     = file.value("mimeType", "");
        info.size         = std::stoll(file.value("size", "0"));
        info.modifiedTime = file.value("modifiedTime", "");
        info.isFolder     = (info.mimeType == "application/vnd.google-apps.folder");
        files.push_back(info);
    }
    return files;
}

GClient::DirListing GClient::listFiles(const std::string& parent_id) {
    std::string query    = "'" + parent_id + "' in parents and trashed = false";
    std::string endpoint = "/files?q=" + cpr::util::urlEncode(query) +
                           "&fields=files(id,name,mimeType,size,modifiedTime)";

    std::string etag;
    auto response_json = makeRequest(endpoint, "GET", nullptr, &etag);

    DirListing result;
    result.files = parseFileList(response_json);
    result.etag  = etag;

    spdlog::debug("Listed {} files from parent '{}' (ETag: {})", result.files.size(), parent_id,
                  etag.empty() ? "none" : etag);
    return result;
}

std::optional<GClient::DirListing> GClient::revalidateDir(const std::string& parent_id,
                                                          const std::string& etag) {
    std::string query    = "'" + parent_id + "' in parents and trashed = false";
    std::string endpoint = "/files?q=" + cpr::util::urlEncode(query) +
                           "&fields=files(id,name,mimeType,size,modifiedTime)";

    std::string new_etag;
    auto response_json = makeRequest(endpoint, "GET", nullptr, &new_etag, etag);

    // makeRequest returns an empty object on 304
    if (response_json.is_null() || response_json.empty()) {
        spdlog::debug("Dir '{}' cache still valid (ETag match)", parent_id);
        return std::nullopt;
    }

    DirListing result;
    result.files = parseFileList(response_json);
    result.etag  = new_etag;
    spdlog::debug("Dir '{}' changed, fetched {} files (new ETag: {})", parent_id,
                  result.files.size(), new_etag.empty() ? "none" : new_etag);
    return result;
}

GClient::FileInfo GClient::getFileMetadata(const std::string& file_id) {
    std::string endpoint = "/files/" + file_id + "?fields=id,name,mimeType,size,modifiedTime";
    auto response_json   = makeRequest(endpoint);

    FileInfo info;
    info.id           = response_json.value("id", "");
    info.name         = response_json.value("name", "");
    info.mimeType     = response_json.value("mimeType", "");
    info.size         = std::stoll(response_json.value("size", "0"));
    info.modifiedTime = response_json.value("modifiedTime", "");
    info.isFolder     = (info.mimeType == "application/vnd.google-apps.folder");

    spdlog::debug("Retrieved metadata for file '{}'", file_id);
    return info;
}

std::string GClient::downloadFile(const std::string& file_id) {
    std::string token = auth_->getAccessToken();
    if (token.empty()) {
        spdlog::error("No access token available");
        return "";
    }

    std::string url = API_BASE_URL + "/files/" + file_id + "?alt=media";

    auto response = cpr::Get(cpr::Url{url}, cpr::Bearer{token});

    if (response.status_code != 200) {
        spdlog::error("File download failed: {}", response.text);
        return "";
    }

    spdlog::debug("Downloaded file '{}' ({} bytes)", file_id, response.text.size());
    return response.text;
}

std::string GClient::uploadFile(const std::string& name, const std::string& content,
                                const std::string& parent_id) {
    std::string token = auth_->getAccessToken();
    if (token.empty()) {
        spdlog::error("No access token available");
        return "";
    }

    // Create file metadata
    nlohmann::json metadata;
    metadata["name"]    = name;
    metadata["parents"] = nlohmann::json::array({parent_id});

    // Use multipart upload
    std::string boundary = "===============7330845974216740156==";
    std::string body     = "--" + boundary + "\r\n";
    body += "Content-Type: application/json; charset=UTF-8\r\n\r\n";
    body += metadata.dump() + "\r\n";
    body += "--" + boundary + "\r\n";
    body += "Content-Type: application/octet-stream\r\n\r\n";
    body += content + "\r\n";
    body += "--" + boundary + "--";

    auto response = cpr::Post(
        cpr::Url{"https://www.googleapis.com/upload/drive/v3/files?uploadType=multipart"},
        cpr::Bearer{token},
        cpr::Header{{"Content-Type", "multipart/related; boundary=" + boundary}}, cpr::Body{body});

    if (response.status_code != 200) {
        spdlog::error("File upload failed: {}", response.text);
        return "";
    }

    auto response_json  = nlohmann::json::parse(response.text);
    std::string file_id = response_json.value("id", "");

    spdlog::debug("Uploaded file '{}' with ID '{}'", name, file_id);
    return file_id;
}

bool GClient::deleteFile(const std::string& file_id) {
    std::string token = auth_->getAccessToken();
    if (token.empty()) {
        spdlog::error("No access token available");
        return false;
    }

    std::string url = API_BASE_URL + "/files/" + file_id;

    auto response = cpr::Delete(cpr::Url{url}, cpr::Bearer{token});

    if (response.status_code != 204) {
        spdlog::error("File deletion failed: {}", response.text);
        return false;
    }

    spdlog::debug("Deleted file '{}'", file_id);
    return true;
}

nlohmann::json GClient::makeRequest(const std::string& endpoint, const std::string& method,
                                    const nlohmann::json& body, std::string* etag_out,
                                    const std::string& if_none_match) {
    std::string token = auth_->getAccessToken();
    if (token.empty()) {
        spdlog::error("No access token available");
        return nlohmann::json();
    }

    std::string url = API_BASE_URL + endpoint;

    cpr::Header extra_headers;
    if (!if_none_match.empty()) {
        extra_headers["If-None-Match"] = if_none_match;
    }

    cpr::Response response;

    if (method == "GET") {
        response = cpr::Get(cpr::Url{url}, cpr::Bearer{token}, extra_headers);
    } else if (method == "POST") {
        response =
            cpr::Post(cpr::Url{url}, cpr::Bearer{token},
                      cpr::Header{{"Content-Type", "application/json"}}, cpr::Body{body.dump()});
    } else if (method == "PATCH") {
        response =
            cpr::Patch(cpr::Url{url}, cpr::Bearer{token},
                       cpr::Header{{"Content-Type", "application/json"}}, cpr::Body{body.dump()});
    } else if (method == "DELETE") {
        response = cpr::Delete(cpr::Url{url}, cpr::Bearer{token});
    } else {
        spdlog::error("Unsupported HTTP method: {}", method);
        return nlohmann::json();
    }

    // 304 Not Modified – cache is still valid, return empty sentinel
    if (response.status_code == 304) {
        return nlohmann::json();  // caller checks .empty()
    }

    if (response.status_code < 200 || response.status_code >= 300) {
        spdlog::error("API request failed: {} - {}", response.status_code, response.text);
        return nlohmann::json();
    }

    // Capture ETag from response if requested
    if (etag_out) {
        auto it = response.header.find("etag");
        if (it != response.header.end()) {
            *etag_out = it->second;
        }
    }

    if (response.text.empty()) {
        return nlohmann::json();
    }

    return nlohmann::json::parse(response.text);
}

std::string GClient::getStartPageToken() {
    std::string token = auth_->getAccessToken();
    if (token.empty()) {
        spdlog::error("getStartPageToken: no access token");
        return "";
    }

    auto response = cpr::Get(cpr::Url{API_BASE_URL + "/changes/startPageToken"}, cpr::Bearer{token},
                             cpr::Parameters{{"supportsAllDrives", "false"}});

    if (response.status_code != 200) {
        spdlog::error("getStartPageToken failed: {} - {}", response.status_code, response.text);
        return "";
    }

    try {
        return nlohmann::json::parse(response.text).value("startPageToken", "");
    } catch (const std::exception& e) {
        spdlog::error("getStartPageToken parse error: {}", e.what());
        return "";
    }
}

GClient::PollResult GClient::pollChanges(const std::string& page_token) {
    std::string token = auth_->getAccessToken();
    if (token.empty()) {
        spdlog::error("pollChanges: no access token");
        return {};
    }

    auto response = cpr::Get(
        cpr::Url{API_BASE_URL + "/changes"}, cpr::Bearer{token},
        cpr::Parameters{
            {"pageToken", page_token},
            {"fields", "nextPageToken,newStartPageToken,changes(fileId,removed,file/parents)"},
            {"restrictToMyDrive", "true"},
            {"spaces", "drive"}});

    if (response.status_code != 200) {
        spdlog::error("pollChanges failed: {} - {}", response.status_code, response.text);
        return {};
    }

    try {
        auto json = nlohmann::json::parse(response.text);

        PollResult result;
        std::unordered_set<std::string> seen;

        for (const auto& change : json.value("changes", nlohmann::json::array())) {
            if (change.value("removed", false)) {
                // Parent unknown for tombstones – invalidate root as fallback
                if (seen.insert("root").second)
                    result.changed_parent_ids.push_back("root");
                continue;
            }
            if (change.contains("file") && change["file"].contains("parents")) {
                for (const auto& p : change["file"]["parents"]) {
                    std::string pid = p.get<std::string>();
                    if (seen.insert(pid).second)
                        result.changed_parent_ids.push_back(pid);
                }
            }
        }

        // Prefer newStartPageToken (end of stream); fall back to nextPageToken
        if (json.contains("newStartPageToken"))
            result.next_page_token = json["newStartPageToken"].get<std::string>();
        else if (json.contains("nextPageToken"))
            result.next_page_token = json["nextPageToken"].get<std::string>();

        return result;
    } catch (const std::exception& e) {
        spdlog::error("pollChanges parse error: {}", e.what());
        return {};
    }
}

void GClient::watcherLoop(ChangeCallback callback, int interval_seconds) {
    spdlog::info("Change watcher starting (interval: {}s)", interval_seconds);

    std::string page_token = getStartPageToken();
    if (page_token.empty()) {
        spdlog::error("Change watcher: failed to obtain startPageToken, aborting");
        return;
    }
    spdlog::debug("Change watcher: startPageToken={}", page_token);

    while (watcher_running_) {
        {
            std::unique_lock<std::mutex> lock(watcher_cv_mutex_);
            watcher_cv_.wait_for(lock, std::chrono::seconds(interval_seconds),
                                 [this] { return !watcher_running_.load(); });
        }
        if (!watcher_running_)
            break;

        try {
            auto result = pollChanges(page_token);
            if (!result.next_page_token.empty())
                page_token = result.next_page_token;

            if (!result.changed_parent_ids.empty()) {
                spdlog::info("Change watcher: {} parent(s) changed",
                             result.changed_parent_ids.size());
                callback(result.changed_parent_ids);
            }
        } catch (const std::exception& e) {
            spdlog::error("Change watcher poll error: {}", e.what());
        }
    }

    spdlog::info("Change watcher stopped");
}

void GClient::startChangeWatcher(ChangeCallback callback, int interval_seconds) {
    stopChangeWatcher();
    watcher_running_ = true;
    watcher_thread_ =
        std::thread(&GClient::watcherLoop, this, std::move(callback), interval_seconds);
}

void GClient::stopChangeWatcher() {
    watcher_running_ = false;
    watcher_cv_.notify_all();
    if (watcher_thread_.joinable())
        watcher_thread_.join();
}

}  // namespace gdrive_fuse
