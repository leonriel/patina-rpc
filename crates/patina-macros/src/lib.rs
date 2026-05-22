//! Procedural macros for Patina RPC.
//!
//! Exposes `#[patina::service]` (re-exported by `patina-rpc` as
//! `#[patina_rpc::service]`), which turns an async trait into a typed RPC
//! client proxy and a server dispatcher. See `DESIGN.md` "Phase 3" for the
//! design and `crates/patina-rpc` for the runtime the generated code targets.

mod expand;
mod model;

use proc_macro::TokenStream;

/// Generate an RPC client + server from an async trait.
///
/// ```ignore
/// #[patina_rpc::service]
/// pub trait Calc: Send + Sync + 'static {
///     async fn add(&self, a: i32, b: i32) -> Result<i32, PatinaError>;
/// }
/// ```
///
/// Produces a `CalcClient` (implements `Calc`, talks to a server over the
/// multiplexer) and a `CalcServer<S>` (wraps a user `impl Calc` and plugs into
/// `ServerBuilder::add_service`).
#[proc_macro_attribute]
pub fn service(_attr: TokenStream, item: TokenStream) -> TokenStream {
    match model::ServiceTrait::parse(item.into()) {
        Ok(service) => expand::expand(&service).into(),
        Err(e) => e.to_compile_error().into(),
    }
}
