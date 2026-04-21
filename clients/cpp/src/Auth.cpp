#include "gdrive_fuse/Auth.hpp"

#include <cpr/cpr.h>
#include <netinet/in.h>
#include <spdlog/spdlog.h>
#include <sys/socket.h>
#include <sys/wait.h>
#include <unistd.h>

#include <chrono>
#include <fstream>
#include <iostream>
#include <random>
#include <sstream>
#include <thread>

namespace gdrive_fuse {

Auth::Auth(const std::string& client_id, const std::string& client_secret)
    : client_id_(client_id),
      client_secret_(client_secret),
      token_expiry_(0) {
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

    // Start Authorization Code Flow
    if (!startAuthFlow()) {
        spdlog::error("Failed to complete authorization flow");
        return false;
    }

    if (!exchangeCodeForToken()) {
        spdlog::error("Failed to exchange code for token");
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
    auto now      = std::chrono::system_clock::now();
    auto now_time = std::chrono::system_clock::to_time_t(now);
    return now_time < token_expiry_;
}

bool Auth::refreshToken() {
    if (refresh_token_.empty()) {
        return false;
    }

    spdlog::info("Refreshing access token");

    auto response = cpr::Post(cpr::Url{"https://oauth2.googleapis.com/token"},
                              cpr::Payload{{"client_id", client_id_},
                                           {"client_secret", client_secret_},
                                           {"refresh_token", refresh_token_},
                                           {"grant_type", "refresh_token"}});

    if (response.status_code != 200) {
        spdlog::error("Token refresh failed: {}", response.text);
        return false;
    }

    auto json     = nlohmann::json::parse(response.text);
    access_token_ = json["access_token"];

    auto now      = std::chrono::system_clock::now();
    auto expiry   = now + std::chrono::seconds(json.value("expires_in", 3600));
    token_expiry_ = std::chrono::system_clock::to_time_t(expiry);

    saveTokens(".gdrive_tokens.json");

    spdlog::info("Token refreshed successfully");
    return true;
}

bool Auth::startAuthFlow() {
    // Generate random state for CSRF protection
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<uint32_t> dis;
    std::ostringstream oss;
    oss << std::hex << dis(gen) << dis(gen);
    state_ = oss.str();

    std::string redirect_uri = "http://localhost:" + std::to_string(redirect_port_);
    std::string scope        = "https://www.googleapis.com/auth/drive";

    std::string auth_url =
        "https://accounts.google.com/o/oauth2/v2/auth"
        "?response_type=code"
        "&client_id=" +
        cpr::util::urlEncode(client_id_) + "&redirect_uri=" + cpr::util::urlEncode(redirect_uri) +
        "&scope=" + cpr::util::urlEncode(scope) +
        "&access_type=offline"
        "&state=" +
        state_;

    std::cout << "\nDann diesen Link im Browser oeffnen:\n" << auth_url << "\n" << std::endl;
    spdlog::info("Open this URL in your browser to authenticate");

    // Best-effort: try to open browser automatically
    pid_t pid = fork();
    if (pid == 0) {
        execlp("xdg-open", "xdg-open", auth_url.c_str(), nullptr);
        _exit(1);
    } else if (pid > 0) {
        // Parent continues without waiting
        (void)pid;
    }

    return waitForCallback();
}

bool Auth::waitForCallback() {
    int server_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (server_fd < 0) {
        spdlog::error("Failed to create socket");
        return false;
    }

    int opt = 1;
    setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    struct sockaddr_in addr {};
    addr.sin_family      = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    addr.sin_port        = htons(static_cast<uint16_t>(redirect_port_));

    if (bind(server_fd, reinterpret_cast<struct sockaddr*>(&addr), sizeof(addr)) < 0) {
        spdlog::error("Failed to bind to port {}", redirect_port_);
        close(server_fd);
        return false;
    }

    if (listen(server_fd, 1) < 0) {
        spdlog::error("Failed to listen on port {}", redirect_port_);
        close(server_fd);
        return false;
    }

    struct timeval timeout {};
    timeout.tv_sec  = 120;
    timeout.tv_usec = 0;
    setsockopt(server_fd, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout));

    spdlog::info("Waiting for browser callback on port {}...", redirect_port_);

    int client_fd = accept(server_fd, nullptr, nullptr);
    close(server_fd);

    if (client_fd < 0) {
        spdlog::error("No callback received within timeout");
        return false;
    }

    char buf[4096]{};
    ssize_t n           = recv(client_fd, buf, sizeof(buf) - 1, 0);
    std::string request = (n > 0) ? std::string(buf, static_cast<size_t>(n)) : "";

    const std::string success_body =
        "<html><body><h1>Authentication successful!</h1>"
        "<p>You can close this tab and return to the terminal.</p></body></html>";
    std::string response =
        "HTTP/1.1 200 OK\r\n"
        "Content-Type: text/html\r\n"
        "Connection: close\r\n\r\n" +
        success_body;
    send(client_fd, response.c_str(), response.size(), 0);
    close(client_fd);

    // Parse ?code=...&state=... from the GET request line
    auto parse_param = [&](const std::string& req, const std::string& param) -> std::string {
        std::string search = param + "=";
        auto pos           = req.find(search);
        if (pos == std::string::npos)
            return "";
        pos += search.size();
        auto end = req.find_first_of("& \t\r\n", pos);
        return urlDecode(req.substr(pos, end == std::string::npos ? std::string::npos : end - pos));
    };

    std::string code  = parse_param(request, "code");
    std::string state = parse_param(request, "state");

    if (code.empty()) {
        spdlog::error("No authorization code received in callback");
        return false;
    }

    if (state != state_) {
        spdlog::error("State parameter mismatch – possible CSRF attempt");
        return false;
    }

    auth_code_ = code;
    spdlog::debug("Authorization code received");
    return true;
}

bool Auth::exchangeCodeForToken() {
    std::string redirect_uri = "http://localhost:" + std::to_string(redirect_port_);

    auto response = cpr::Post(cpr::Url{"https://oauth2.googleapis.com/token"},
                              cpr::Payload{{"code", auth_code_},
                                           {"client_id", client_id_},
                                           {"client_secret", client_secret_},
                                           {"redirect_uri", redirect_uri},
                                           {"grant_type", "authorization_code"}});

    if (response.status_code != 200) {
        spdlog::error("Token exchange failed: {}", response.text);
        return false;
    }

    auto json     = nlohmann::json::parse(response.text);
    access_token_ = json["access_token"];
    if (json.contains("refresh_token")) {
        refresh_token_ = json["refresh_token"];
    }

    auto now      = std::chrono::system_clock::now();
    auto expiry   = now + std::chrono::seconds(json.value("expires_in", 3600));
    token_expiry_ = std::chrono::system_clock::to_time_t(expiry);

    spdlog::info("Token exchange successful");
    return true;
}

std::string Auth::urlDecode(const std::string& src) {
    std::string decoded;
    decoded.reserve(src.size());
    for (size_t i = 0; i < src.size(); ++i) {
        if (src[i] == '%' && i + 2 < src.size()) {
            char hex[3] = {src[i + 1], src[i + 2], '\0'};
            decoded += static_cast<char>(std::strtol(hex, nullptr, 16));
            i += 2;
        } else if (src[i] == '+') {
            decoded += ' ';
        } else {
            decoded += src[i];
        }
    }
    return decoded;
}

bool Auth::saveTokens(const std::string& filepath) {
    nlohmann::json json;
    json["access_token"]  = access_token_;
    json["refresh_token"] = refresh_token_;
    json["token_expiry"]  = token_expiry_;

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

    access_token_  = json.value("access_token", "");
    refresh_token_ = json.value("refresh_token", "");
    token_expiry_  = json.value("token_expiry", 0);

    spdlog::info("Tokens loaded from {}", filepath);
    return true;
}

}  // namespace gdrive_fuse
