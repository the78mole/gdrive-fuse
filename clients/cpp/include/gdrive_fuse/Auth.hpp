#pragma once

#include <nlohmann/json.hpp>

#include <memory>
#include <mutex>
#include <string>

namespace gdrive_fuse {

/**
 * @brief Handles OAuth2 Authorization Code Flow for desktop authentication
 *
 * Implements the OAuth2 Authorization Code Flow with a localhost redirect.
 * Opens the authorization URL in the browser and starts a temporary local
 * HTTP server to capture the callback. Thread-safe.
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
     * @brief Initiate the authorization code flow
     *
     * Tries cached tokens first, refreshes if expired, or starts a new
     * browser-based OAuth2 flow with a localhost callback server.
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
    std::string auth_code_;
    std::string state_;
    int redirect_port_{8080};
    long token_expiry_{0};

    mutable std::mutex mutex_;  // For thread-safety

    bool startAuthFlow();
    bool waitForCallback();
    bool exchangeCodeForToken();
    bool saveTokens(const std::string& filepath);
    bool loadTokens(const std::string& filepath);
    static std::string urlDecode(const std::string& src);
};

}  // namespace gdrive_fuse
