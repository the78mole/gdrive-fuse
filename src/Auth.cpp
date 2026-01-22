#include "gdrive_fuse/Auth.hpp"
#include <spdlog/spdlog.h>
#include <cpr/cpr.h>
#include <fstream>
#include <chrono>

namespace gdrive_fuse {

Auth::Auth(const std::string& client_id, const std::string& client_secret)
    : client_id_(client_id)
    , client_secret_(client_secret)
    , token_expiry_(0) {
    spdlog::debug("Auth object created");
}

Auth::~Auth() {
    spdlog::debug("Auth object destroyed");
}

bool Auth::authenticate() {
    std::lock_guard<std::mutex> lock(mutex_);
    
    // Try to load existing tokens first
    if (loadTokens(".gdrive_tokens.json")) {
        if (isTokenValid()) {
            spdlog::info("Loaded valid tokens from cache");
            return true;
        }
        // Try to refresh if expired
        if (!refresh_token_.empty() && refreshToken()) {
            return true;
        }
    }
    
    // Start device flow
    if (!requestDeviceCode()) {
        spdlog::error("Failed to request device code");
        return false;
    }
    
    spdlog::info("Please visit: {} and enter code: {}", verification_url_, user_code_);
    
    // Poll for token
    if (!pollForToken()) {
        spdlog::error("Failed to obtain token");
        return false;
    }
    
    // Save tokens
    saveTokens(".gdrive_tokens.json");
    
    return true;
}

std::string Auth::getAccessToken() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return access_token_;
}

bool Auth::isTokenValid() const {
    auto now = std::chrono::system_clock::now();
    auto now_time = std::chrono::system_clock::to_time_t(now);
    return now_time < token_expiry_;
}

bool Auth::refreshToken() {
    if (refresh_token_.empty()) {
        return false;
    }
    
    spdlog::info("Refreshing access token");
    
    auto response = cpr::Post(
        cpr::Url{"https://oauth2.googleapis.com/token"},
        cpr::Payload{
            {"client_id", client_id_},
            {"client_secret", client_secret_},
            {"refresh_token", refresh_token_},
            {"grant_type", "refresh_token"}
        }
    );
    
    if (response.status_code != 200) {
        spdlog::error("Token refresh failed: {}", response.text);
        return false;
    }
    
    auto json = nlohmann::json::parse(response.text);
    access_token_ = json["access_token"];
    
    auto now = std::chrono::system_clock::now();
    auto expiry = now + std::chrono::seconds(json.value("expires_in", 3600));
    token_expiry_ = std::chrono::system_clock::to_time_t(expiry);
    
    saveTokens(".gdrive_tokens.json");
    
    spdlog::info("Token refreshed successfully");
    return true;
}

bool Auth::requestDeviceCode() {
    auto response = cpr::Post(
        cpr::Url{"https://oauth2.googleapis.com/device/code"},
        cpr::Payload{
            {"client_id", client_id_},
            {"scope", "https://www.googleapis.com/auth/drive"}
        }
    );
    
    if (response.status_code != 200) {
        spdlog::error("Device code request failed: {}", response.text);
        return false;
    }
    
    auto json = nlohmann::json::parse(response.text);
    device_code_ = json["device_code"];
    user_code_ = json["user_code"];
    verification_url_ = json["verification_url"];
    
    return true;
}

bool Auth::pollForToken() {
    const int max_attempts = 60; // 5 minutes with 5 second intervals
    const int interval = 5;
    
    for (int attempt = 0; attempt < max_attempts; ++attempt) {
        std::this_thread::sleep_for(std::chrono::seconds(interval));
        
        auto response = cpr::Post(
            cpr::Url{"https://oauth2.googleapis.com/token"},
            cpr::Payload{
                {"client_id", client_id_},
                {"client_secret", client_secret_},
                {"device_code", device_code_},
                {"grant_type", "urn:ietf:params:oauth:grant-type:device_code"}
            }
        );
        
        if (response.status_code == 200) {
            auto json = nlohmann::json::parse(response.text);
            access_token_ = json["access_token"];
            refresh_token_ = json["refresh_token"];
            
            auto now = std::chrono::system_clock::now();
            auto expiry = now + std::chrono::seconds(json.value("expires_in", 3600));
            token_expiry_ = std::chrono::system_clock::to_time_t(expiry);
            
            spdlog::info("Authentication successful");
            return true;
        }
        
        auto json = nlohmann::json::parse(response.text);
        std::string error = json.value("error", "unknown");
        
        if (error == "authorization_pending") {
            spdlog::debug("Waiting for user authorization... (attempt {}/{})", attempt + 1, max_attempts);
            continue;
        } else if (error == "slow_down") {
            // Increase interval
            std::this_thread::sleep_for(std::chrono::seconds(interval));
            continue;
        } else {
            spdlog::error("Token polling failed: {}", error);
            return false;
        }
    }
    
    spdlog::error("Token polling timeout");
    return false;
}

bool Auth::saveTokens(const std::string& filepath) {
    nlohmann::json json;
    json["access_token"] = access_token_;
    json["refresh_token"] = refresh_token_;
    json["token_expiry"] = token_expiry_;
    
    std::ofstream file(filepath);
    if (!file.is_open()) {
        spdlog::error("Failed to save tokens to {}", filepath);
        return false;
    }
    
    file << json.dump(4);
    spdlog::info("Tokens saved to {}", filepath);
    return true;
}

bool Auth::loadTokens(const std::string& filepath) {
    std::ifstream file(filepath);
    if (!file.is_open()) {
        spdlog::debug("No token file found at {}", filepath);
        return false;
    }
    
    nlohmann::json json;
    file >> json;
    
    access_token_ = json.value("access_token", "");
    refresh_token_ = json.value("refresh_token", "");
    token_expiry_ = json.value("token_expiry", 0);
    
    spdlog::info("Tokens loaded from {}", filepath);
    return true;
}

} // namespace gdrive_fuse
