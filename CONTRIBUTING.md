# Contributing to gdrive-fuse

Thank you for taking the time to contribute! This document describes the process for reporting bugs, proposing features, and submitting code changes.

---

## Table of Contents

- [Contributing to gdrive-fuse](#contributing-to-gdrive-fuse)
  - [Table of Contents](#table-of-contents)
  - [Code of Conduct](#code-of-conduct)
  - [How to Report a Bug](#how-to-report-a-bug)
  - [How to Request a Feature](#how-to-request-a-feature)
  - [Development Setup](#development-setup)
    - [Recommended tools](#recommended-tools)
  - [Coding Guidelines](#coding-guidelines)
  - [Commit Messages](#commit-messages)
  - [Pull Request Process](#pull-request-process)
  - [Security Vulnerabilities](#security-vulnerabilities)

---

## Code of Conduct

This project follows the [Contributor Covenant v2.1](https://www.contributor-covenant.org/version/2/1/code_of_conduct/).  
Please be respectful and constructive in all interactions.

---

## How to Report a Bug

Use the **Bug Report** issue template on GitHub.  
Before filing, please:

- Search [existing issues](https://github.com/the78mole/gdrive-fuse/issues) to avoid duplicates.
- Reproduce the problem with the latest build from `main`.
- Include the full debug log (`--debug` flag) with credentials redacted.

---

## How to Request a Feature

Use the **Feature Request** issue template or open a **GitHub Discussion** in the *Ideas* category first if you want to gauge interest before writing code.

---

## Development Setup

```bash
git clone https://github.com/the78mole/gdrive-fuse.git
cd gdrive-fuse
make build          # Debug build + generates compile_commands.json
make install-hooks  # Installs pre-commit (pre-commit + commit-msg stages)
```

See [docs/BUILD.md](docs/BUILD.md) for all build prerequisites and credential setup.

### Recommended tools

| Tool | Purpose |
|---|---|
| `clang-format` 18+ | Code formatting (config in `.clang-format`) |
| `clang-tidy` | Static analysis (config in `.clang-tidy`) |
| `pre-commit` | Runs all hooks automatically on `git commit` |
| `valgrind` / `AddressSanitizer` | Memory error detection |
| `gdb` | Debugging FUSE processes |

---

## Coding Guidelines

- **Language standard:** C++20, no extensions (`-std=c++20 -pedantic`).
- **Formatting:** Run `make format` (clang-format) before committing. The pre-commit hook enforces this automatically.
- **Linting:** Run `make lint` (clang-tidy) or `make lint-hooks` (all pre-commit checks) before opening a PR.
- **Thread safety:** All public `GClient` and `FuseOps` methods must be safe to call from multiple threads. Use `std::lock_guard` / `std::unique_lock` with the existing `mutex_` members.
- **Error handling:** Log errors via `spdlog::error()`; return POSIX error codes from FUSE callbacks (negative `errno` values).
- **No raw owning pointers:** Use `std::shared_ptr` / `std::unique_ptr`.
- **No exceptions across the FUSE boundary:** Catch all exceptions inside FUSE callbacks and map them to `-EIO` or the appropriate errno.
- **Credentials:** Never commit `credentials.json`, `.gdrive_tokens.json`, or any token/secret. Both files are in `.gitignore`.

---

## Commit Messages

Follow the [Conventional Commits](https://www.conventionalcommits.org/) specification:

```
<type>(<scope>): <short summary>

[optional body]

[optional footer(s)]
```

**Types:** `feat`, `fix`, `refactor`, `perf`, `docs`, `test`, `chore`, `ci`

**Examples:**

```
feat(cache): add ETag-based revalidation for directory listings
fix(gclient): parse Drive API size field as string not integer
docs(build): add fuse group permission troubleshooting note
```

Limit the subject line to **72 characters**. Reference related issues with `Closes #<n>` or `Relates to #<n>` in the footer.

---

## Pull Request Process

1. Fork the repository and create a branch from `main`:
   ```bash
   git checkout -b fix/my-bug-fix
   ```
2. Make your changes, add or update tests where applicable.
3. Ensure the project builds cleanly with no new warnings:
   ```bash
   make build 2>&1 | grep -E "warning:|error:"
   ```
4. Push your branch and open a Pull Request against `main`.
5. Fill in the PR template completely.
6. At least one maintainer review is required before merging.
7. Squash-merge is preferred for feature branches; fast-forward for small fixes.

---

## Security Vulnerabilities

**Do not open a public issue for security vulnerabilities.**  
Please report them privately via [GitHub Security Advisories](https://github.com/the78mole/gdrive-fuse/security/advisories/new) or by emailing the maintainer directly.  
We aim to respond within 72 hours.
