#pragma once

#include <string>
#include <memory>
#include <mutex>
#include <nlohmann/json.hpp>

namespace gdrive_fuse {

/**
 * @brief Handles OAuth2 Device Authorization Flow for headless authentication
 * 
 * This class implements the OAuth2 Device Authorization Flow which is suitable
 * for headless environments where a browser is not available. It is thread-safe.
 */
class Auth {
public:
    /**
     * @brief Construct a new Auth object
     * 
     * @param client_id OAuth2 client ID
     * @param client_secret OAuth2 client secret
     */
    Auth(const std::string& client_id, const std::string& client_secret);
    
    /**
     * @brief Destroy the Auth object
     */
    ~Auth();

    /**
     * @brief Initiate the device authorization flow
     * 
     * @return true if successful
     * @return false otherwise
     */
    bool authenticate();

    /**
     * @brief Get the access token
     * 
     * @return std::string The current access token
     */
    std::string getAccessToken() const;

    /**
     * @brief Check if the current token is valid
     * 
     * @return true if token is valid
     * @return false otherwise
     */
    bool isTokenValid() const;

    /**
     * @brief Refresh the access token
     * 
     * @return true if successful
     * @return false otherwise
     */
    bool refreshToken();

private:
    std::string client_id_;
    std::string client_secret_;
    std::string access_token_;
    std::string refresh_token_;
    std::string device_code_;
    std::string user_code_;
    std::string verification_url_;
    long token_expiry_;
    
    mutable std::mutex mutex_;  // For thread-safety
    
    bool requestDeviceCode();
    bool pollForToken();
    bool saveTokens(const std::string& filepath);
    bool loadTokens(const std::string& filepath);
};

} // namespace gdrive_fuse
