# Changelog

## [4.1.1]

### Fixed

- Fixed signaling exiting read loop.
- Avoid leaking watch threads on client shutdown.

## [4.1.0]

### Fixed

- Fixed compatibility with old ZK clusters.

### New

- Distributed queue recipe.

## [4.0.1]

### Fixed

- Fix lock not setting handler on first node in leader election.

### Changed

- Updated dependencies

## [4.0.0]

### Changed

- Updated `tokio` to 1.0

## [3.0.0]

### Changed

- Updated `tokio` to 0.3

### New

- To string conversion from some enums

## [2.1.0]

### New

- New [leader latch](https://curator.apache.org/curator-recipes/leader-latch.html) recipe

## [2.0.0]

### Changed

- Node watchers are now `FnOnce`
- Added missing traits for enums: `Ord`, `PartialOrd`, `Eq`, `Hash`
- `WatchedEventType` and `KeeperState` enums now implement `TryFrom<i32>` instead of `From<i32>`
- Using `tracing` for logs instead of `log`

### New

- New `ensure_path_with_leaf_mode` function (useful e.g. for creating containers for locks)
- New [shared lock](https://curator.apache.org/curator-recipes/shared-lock.html) recipe

## [1.0.3]

### Changed

- Rewritten legacy io system to avoid busy waiting

