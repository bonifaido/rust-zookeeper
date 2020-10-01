# Changelog

## [x.x.x]

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

