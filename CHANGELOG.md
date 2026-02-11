# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.2.0] - 2026-02-11

### Added

- `espg::Id<T>` can now be used with `async_graphql` contexts without implementing `espg::Aggregate`
- `espg::Id<T>` can now be passed by reference to `EventStore::load` to keep ownership at call site
- `uuid` feature flag
- `espg::Id<T>` supports `From<&str>`, `From<&String>`, `From<String>` and `From<uuid::Uuid>` (behind `uuid` feature flag)
- `EventStore::all<X: Aggregate>()` to load all aggregates
- `EventStore::create<X: Aggregate>(event: X::Event) -> Id<X>` to create an aggregate (behind `uuid` feature flag)
- Support `FromRequest` for `EventStore<rocket_db_pools::Connection<D: rocket_db_pools::Database>>` using `deadpool_postgres` (behind `rocket_db_pools` feature flag)
- `EventStore::get_subscription_state` (behind `streaming` flag)

### Changed

- Renamed `espg::Transaction::commit` to `finish` to allow access to `commit` of the underlying `EventStore` object
- Marked `FromParam::Error` for `espg::Id<T>` as `Infallible`
- `espg::Id<T>` now differs bet ween `Debug` and `Display` where the former will output with format `ID<T>(value)`
- `pgmanager` was extracted to its own repository
- `espg::id` function is deprecated in favor of using `Into<espg::Id>`
- Feature flagged `impl Loadable foor IndexMap` and `indexmap` direct dependency behind `indexmap`

## [0.1.0] - 2025-11-19

### Added

- Initial release

[unreleased]: https://github.com/onelittle/espg/compare/v0.2.0...HEAD
[0.2.0]: https://github.com/onelittle/espg/releases/tag/v0.2.0
[0.1.0]: https://github.com/onelittle/espg/releases/tag/v0.1.0
