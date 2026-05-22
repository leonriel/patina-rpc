//! Code generation for `#[service]` (`DESIGN.md` Phase 3 §2.2).
//!
//! For a trait `Calc`, emits three artifacts in the user's module:
//!   1. the trait itself, desugared from `async fn` to `fn -> impl Future + Send`
//!      (native RPITIT, so user impls need no `#[async_trait]`);
//!   2. `CalcClient` — a concrete proxy that implements `Calc` by serializing
//!      args, calling over the multiplexer, and deserializing the reply;
//!   3. `CalcServer<S>` — wraps a user `impl Calc` and implements
//!      `patina_rpc::PatinaService` (the doc's `dispatch_[trait]` match).

use proc_macro2::TokenStream;
use quote::{format_ident, quote};

use crate::model::{Method, ServiceTrait};

pub fn expand(service: &ServiceTrait) -> TokenStream {
    let trait_def = expand_trait(service);
    let client = expand_client(service);
    let server = expand_server(service);
    quote! {
        #trait_def
        #client
        #server
    }
}

/// Re-emit the trait with each `async fn` rewritten to an RPITIT signature.
fn expand_trait(service: &ServiceTrait) -> TokenStream {
    let ServiceTrait { vis, attrs, supertraits, ident, methods } = service;

    let sigs = methods.iter().map(|method| {
        let name = &method.ident;
        let params = method.args.iter().map(|arg| {
            let n = &arg.name;
            let t = &arg.ty;
            quote!(#n: #t)
        });
        let ok = &method.ok_type;
        quote! {
            fn #name(&self #(, #params)*)
                -> impl ::core::future::Future<
                    Output = ::core::result::Result<#ok, ::patina_rpc::PatinaError>
                > + ::core::marker::Send;
        }
    });

    if supertraits.is_empty() {
        quote! {
            #(#attrs)*
            #vis trait #ident {
                #(#sigs)*
            }
        }
    } else {
        quote! {
            #(#attrs)*
            #vis trait #ident: #supertraits {
                #(#sigs)*
            }
        }
    }
}

/// Generate `[Trait]Client`: a concrete proxy implementing the trait.
fn expand_client(service: &ServiceTrait) -> TokenStream {
    let ServiceTrait { vis, ident, methods, .. } = service;
    let client_ident = format_ident!("{}Client", ident);

    let impl_methods = methods.iter().map(|method| {
        let name = &method.ident;
        let params = method.args.iter().map(|arg| {
            let n = &arg.name;
            let t = &arg.ty;
            quote!(#n: #t)
        });
        let arg_names = method.args.iter().map(|arg| &arg.name);
        let ok = &method.ok_type;
        let wire_name = wire_method_name(service, method);
        quote! {
            async fn #name(&self #(, #params)*)
                -> ::core::result::Result<#ok, ::patina_rpc::PatinaError>
            {
                let __payload = ::patina_rpc::__private::encode(&( #(#arg_names,)* ))?;
                let __response = self.inner.call(#wire_name, __payload).await?;
                ::patina_rpc::__private::decode(&__response)
            }
        }
    });

    quote! {
        #vis struct #client_ident {
            inner: ::std::sync::Arc<::patina_rpc::Client>,
        }

        impl #client_ident {
            /// Connect to a Patina server at `addr` and wrap it in this proxy.
            #vis async fn connect<__A>(addr: __A)
                -> ::core::result::Result<Self, ::patina_rpc::PatinaError>
            where
                __A: ::patina_rpc::__private::ToSocketAddrs,
            {
                let __client = ::patina_rpc::Client::connect(addr).await?;
                ::core::result::Result::Ok(Self { inner: ::std::sync::Arc::new(__client) })
            }

            /// Build a proxy from an existing shared `Client` (e.g. to share one
            /// connection across several service proxies).
            #vis fn from_client(inner: ::std::sync::Arc<::patina_rpc::Client>) -> Self {
                Self { inner }
            }
        }

        impl #ident for #client_ident {
            #(#impl_methods)*
        }
    }
}

/// Generate `[Trait]Server<S>`: wraps a user `impl Trait` and implements
/// `PatinaService` with a match over method names.
fn expand_server(service: &ServiceTrait) -> TokenStream {
    let ServiceTrait { vis, ident, methods, .. } = service;
    let server_ident = format_ident!("{}Server", ident);

    let wire_names = methods.iter().map(|method| wire_method_name(service, method));

    let dispatch_arms = methods.iter().map(|method| {
        let name = &method.ident;
        let wire_name = wire_method_name(service, method);
        let arg_names: Vec<_> = method.args.iter().map(|arg| &arg.name).collect();
        let arg_types = method.args.iter().map(|arg| &arg.ty);
        quote! {
            #wire_name => {
                let ( #(#arg_names,)* ): ( #(#arg_types,)* ) =
                    ::patina_rpc::__private::decode(&payload)?;
                let __ret = self.inner.#name( #(#arg_names),* ).await?;
                ::patina_rpc::__private::encode(&__ret).map_err(::core::convert::Into::into)
            }
        }
    });

    quote! {
        #vis struct #server_ident<__S> {
            inner: ::std::sync::Arc<__S>,
        }

        impl<__S: #ident> #server_ident<__S> {
            /// Wrap a service implementation. Pass the result to
            /// `ServerBuilder::add_service`.
            #vis fn new(inner: __S) -> Self {
                Self { inner: ::std::sync::Arc::new(inner) }
            }
        }

        impl<__S> ::patina_rpc::PatinaService for #server_ident<__S>
        where
            __S: #ident + ::core::marker::Send + ::core::marker::Sync + 'static,
        {
            fn method_names(&self) -> &'static [&'static str] {
                &[ #(#wire_names),* ]
            }

            async fn dispatch(&self, method: &str, payload: ::std::vec::Vec<u8>)
                -> ::core::result::Result<::std::vec::Vec<u8>, ::patina_rpc::HandlerError>
            {
                match method {
                    #(#dispatch_arms)*
                    __unknown => ::core::result::Result::Err(
                        ::patina_rpc::HandlerError::new(
                            404,
                            ::std::format!("unknown method: {__unknown}"),
                        ),
                    ),
                }
            }
        }
    }
}

/// The on-the-wire method name, `"Trait::method"` (DESIGN.md §2.2).
fn wire_method_name(service: &ServiceTrait, method: &Method) -> String {
    format!("{}::{}", service.ident, method.ident)
}
