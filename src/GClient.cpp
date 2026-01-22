#include "gdrive_fuse/GClient.hpp"
#include "gdrive_fuse/Auth.hpp"
#include <spdlog/spdlog.h>
#include <cpr/cpr.h>

namespace gdrive_fuse {

const std::string GClient::API_BASE_URL = "https://www.googleapis.com/drive/v3";

GClient::GClient(std::shared_ptr<Auth> auth)
    : auth_(auth) {
    spdlog::debug("GClient object created");
}

GClient::~GClient() {
    spdlog::debug("GClient object destroyed");
}

std::vector<GClient::FileInfo> GClient::listFiles(const std::string& parent_id) {
    std::lock_guard<std::mutex> lock(mutex_);
    
    std::string query = "'" + parent_id + "' in parents and trashed = false";
    std::string endpoint = "/files?q=" + cpr::util::urlEncode(query) + 
                          "&fields=files(id,name,mimeType,size,modifiedTime)";
    
    auto response_json = makeRequest(endpoint);
    
    std::vector<FileInfo> files;
    if (response_json.contains("files")) {
        for (const auto& file : response_json["files"]) {
            FileInfo info;
            info.id = file.value("id", "");
            info.name = file.value("name", "");
            info.mimeType = file.value("mimeType", "");
            info.size = file.value("size", 0);
            info.modifiedTime = file.value("modifiedTime", "");
            info.isFolder = (info.mimeType == "application/vnd.google-apps.folder");
            files.push_back(info);
        }
    }
    
    spdlog::debug("Listed {} files from parent '{}'", files.size(), parent_id);
    return files;
}

GClient::FileInfo GClient::getFileMetadata(const std::string& file_id) {
    std::lock_guard<std::mutex> lock(mutex_);
    
    std::string endpoint = "/files/" + file_id + "?fields=id,name,mimeType,size,modifiedTime";
    auto response_json = makeRequest(endpoint);
    
    FileInfo info;
    info.id = response_json.value("id", "");
    info.name = response_json.value("name", "");
    info.mimeType = response_json.value("mimeType", "");
    info.size = response_json.value("size", 0);
    info.modifiedTime = response_json.value("modifiedTime", "");
    info.isFolder = (info.mimeType == "application/vnd.google-apps.folder");
    
    spdlog::debug("Retrieved metadata for file '{}'", file_id);
    return info;
}

std::string GClient::downloadFile(const std::string& file_id) {
    std::lock_guard<std::mutex> lock(mutex_);
    
    std::string token = auth_->getAccessToken();
    if (token.empty()) {
        spdlog::error("No access token available");
        return "";
    }
    
    std::string url = API_BASE_URL + "/files/" + file_id + "?alt=media";
    
    auto response = cpr::Get(
        cpr::Url{url},
        cpr::Bearer{token}
    );
    
    if (response.status_code != 200) {
        spdlog::error("File download failed: {}", response.text);
        return "";
    }
    
    spdlog::debug("Downloaded file '{}' ({} bytes)", file_id, response.text.size());
    return response.text;
}

std::string GClient::uploadFile(const std::string& name, const std::string& content,
                               const std::string& parent_id) {
    std::lock_guard<std::mutex> lock(mutex_);
    
    std::string token = auth_->getAccessToken();
    if (token.empty()) {
        spdlog::error("No access token available");
        return "";
    }
    
    // Create file metadata
    nlohmann::json metadata;
    metadata["name"] = name;
    metadata["parents"] = nlohmann::json::array({parent_id});
    
    // Use multipart upload
    std::string boundary = "===============7330845974216740156==";
    std::string body = "--" + boundary + "\r\n";
    body += "Content-Type: application/json; charset=UTF-8\r\n\r\n";
    body += metadata.dump() + "\r\n";
    body += "--" + boundary + "\r\n";
    body += "Content-Type: application/octet-stream\r\n\r\n";
    body += content + "\r\n";
    body += "--" + boundary + "--";
    
    auto response = cpr::Post(
        cpr::Url{"https://www.googleapis.com/upload/drive/v3/files?uploadType=multipart"},
        cpr::Bearer{token},
        cpr::Header{{"Content-Type", "multipart/related; boundary=" + boundary}},
        cpr::Body{body}
    );
    
    if (response.status_code != 200) {
        spdlog::error("File upload failed: {}", response.text);
        return "";
    }
    
    auto response_json = nlohmann::json::parse(response.text);
    std::string file_id = response_json.value("id", "");
    
    spdlog::debug("Uploaded file '{}' with ID '{}'", name, file_id);
    return file_id;
}

bool GClient::deleteFile(const std::string& file_id) {
    std::lock_guard<std::mutex> lock(mutex_);
    
    std::string token = auth_->getAccessToken();
    if (token.empty()) {
        spdlog::error("No access token available");
        return false;
    }
    
    std::string url = API_BASE_URL + "/files/" + file_id;
    
    auto response = cpr::Delete(
        cpr::Url{url},
        cpr::Bearer{token}
    );
    
    if (response.status_code != 204) {
        spdlog::error("File deletion failed: {}", response.text);
        return false;
    }
    
    spdlog::debug("Deleted file '{}'", file_id);
    return true;
}

nlohmann::json GClient::makeRequest(const std::string& endpoint, const std::string& method,
                                    const nlohmann::json& body) {
    std::string token = auth_->getAccessToken();
    if (token.empty()) {
        spdlog::error("No access token available");
        return nlohmann::json();
    }
    
    std::string url = API_BASE_URL + endpoint;
    
    cpr::Response response;
    
    if (method == "GET") {
        response = cpr::Get(
            cpr::Url{url},
            cpr::Bearer{token}
        );
    } else if (method == "POST") {
        response = cpr::Post(
            cpr::Url{url},
            cpr::Bearer{token},
            cpr::Header{{"Content-Type", "application/json"}},
            cpr::Body{body.dump()}
        );
    } else if (method == "PATCH") {
        response = cpr::Patch(
            cpr::Url{url},
            cpr::Bearer{token},
            cpr::Header{{"Content-Type", "application/json"}},
            cpr::Body{body.dump()}
        );
    } else if (method == "DELETE") {
        response = cpr::Delete(
            cpr::Url{url},
            cpr::Bearer{token}
        );
    }
    
    if (response.status_code < 200 || response.status_code >= 300) {
        spdlog::error("API request failed: {} - {}", response.status_code, response.text);
        return nlohmann::json();
    }
    
    if (response.text.empty()) {
        return nlohmann::json();
    }
    
    return nlohmann::json::parse(response.text);
}

} // namespace gdrive_fuse
