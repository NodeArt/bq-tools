# Changelog

The format is based on [Keep a Changelog](https://keepachangelog.com), and this project adheres to an adapted variant of [Semantic Versioning](https://semver.org/spec/v2.0.0.html) and takes parts of [Conventional Commits Specification](https://www.conventionalcommits.org) as a base.

All changes in this repository will be documented in this file. Types of changes:

- `Added`: for new features.
- `Changed`: for changes in existing functionality.
- `Deprecated`: for soon-to-be removed features.
- `Removed`: for now removed features.
- `Fixed`: for any bug fixes.
- `Security`: in case of vulnerabilities.

Changes that have yet to make it into the release version will be listed under `Unreleased` header and will be moved to the header with and appropriate version and its release date once it is released.

---

## Unreleased

## v1.2.3 - 2026-07-31

### Fixed

- Use the OAuth response `expires_in` value for access token caching

## v1.2.2 - 2026-07-31

### Changed

- Update production and development dependencies
- Update the TypeScript and ESLint configuration for the latest compatible toolchain
- Remove the unused HTML ESLint reporter

## v1.2.1 - 2026-07-31

### Fixed

- Refresh cached OAuth tokens before they expire

## v1.2.0 - 2026-04-27

### Added

- Support for views

## v1.1.1 - 2026-04-15

### Changed

- Throw error when no schemas are found

## v1.1.0 - 2026-04-15

### Added

- Add ability to override project id from env/cli args

## v1.0.3 - 2025-07-03

### Fixed

- Add JWT crypto key `toStringTag`

## v1.0.2 - 2025-07-03

### Fixed

- Fixed JWT sign order

## v1.0.1 - 2025-07-02

### Fixed

- Fixed CLI table name prefix/suffix concatenation

## v1.0.0 - 2025-07-01

### Added

- Added initial implementation.
