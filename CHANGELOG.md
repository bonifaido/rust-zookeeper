# Changelog

## [2.0.0]

### Changed

- Node watchers are now `FnOnce`
- Added missing traits for enums: `Ord`, `PartialOrd`, `Eq`, `Hash`

### New

- New `ensure_path_with_leaf_mode` function (useful e.g. for creating containers for locks)

## [1.0.3]

### Changed

- Rewritten legacy io system to avoid busy waiting

