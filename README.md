# gdrive-fuse

A C++20 CLI-only implementation of Google Drive FUSE filesystem with modern C++ design.

## Features

- **OAuth2 Device Authorization Flow**: Headless authentication suitable for servers and remote systems
- **Google Drive REST API**: Full integration with Google Drive v3 API
- **FUSE3 Support**: Mount Google Drive as a local filesystem
- **Thread-Safe Design**: All operations are thread-safe using modern C++ primitives
- **Modern C++20**: Uses C++20 features and best practices

## Architecture

The project is structured into four main components:

1. **Auth**: Implements OAuth2 Device Authorization Flow for headless authentication
   - Handles token acquisition, refresh, and persistence
   - Thread-safe token management

2. **GClient**: Google Drive REST API wrapper
   - List files and directories
   - Upload and download files
   - Delete files
   - Get file metadata

3. **FuseOps**: FUSE filesystem operations
   - `getattr`: Get file attributes
   - `readdir`: Read directory contents
   - `read`: Read file contents
   - Path-to-ID caching for performance

4. **main**: CLI entry point
   - Command-line argument parsing
   - FUSE initialization
   - Authentication orchestration

## Dependencies

- **FUSE3**: Filesystem in Userspace
- **cpr**: HTTP client library (wrapper around libcurl)
- **nlohmann/json**: JSON parsing and serialization
- **spdlog**: Fast logging library

All dependencies except FUSE3 are automatically fetched using CMake FetchContent.

## Building

### Prerequisites

```bash
# Ubuntu/Debian
sudo apt-get install libfuse3-dev pkg-config cmake build-essential

# Fedora/RHEL
sudo dnf install fuse3-devel pkgconfig cmake gcc-c++

# macOS
brew install macfuse cmake
```

### Build Steps

```bash
mkdir build
cd build
cmake ..
make -j$(nproc)
```

The executable `gdrive-fuse` will be created in the build directory.

## Usage

### Setup Google Cloud OAuth2 Credentials

1. Go to [Google Cloud Console](https://console.cloud.google.com/)
2. Create a new project or select an existing one
3. Enable the Google Drive API
4. Create OAuth2 credentials (Desktop application)
5. Note down your Client ID and Client Secret

### Mount Google Drive

```bash
./gdrive-fuse --client-id YOUR_CLIENT_ID --client-secret YOUR_CLIENT_SECRET /mnt/gdrive
```

On first run, you'll see a URL and code:
```
Please visit: https://www.google.com/device and enter code: XXXX-XXXX
```

Visit the URL in a browser, enter the code, and authorize the application.

### Additional Options

```bash
# Enable debug logging
./gdrive-fuse --client-id ID --client-secret SECRET --debug /mnt/gdrive

# Pass FUSE options
./gdrive-fuse --client-id ID --client-secret SECRET /mnt/gdrive -f -o allow_other
```

### Unmount

```bash
fusermount -u /mnt/gdrive
```

## Thread Safety

All components are designed to be thread-safe:

- **Auth**: Uses `std::mutex` to protect token state
- **GClient**: Uses `std::mutex` for API requests
- **FuseOps**: Uses `std::mutex` for cache operations

FUSE itself may call operations concurrently, so thread safety is critical.

## Security

- Tokens are stored in `.gdrive_tokens.json` in the current directory
- Make sure to protect this file with appropriate permissions
- Never commit this file to version control

## License

MIT License - See LICENSE file for details

## Contributing

Contributions are welcome! Please ensure:
- Code follows C++20 best practices
- Thread safety is maintained
- Changes are tested with FUSE operations
