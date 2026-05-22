//! Parse + validate the annotated trait into a structured model.
//!
//! Enforces the constraints from `DESIGN.md` Phase 3 §2.1:
//!   * the item is a trait;
//!   * every method is `async` with no default body;
//!   * the receiver is `&self` (never `&mut self` or `self`);
//!   * the return type is `Result<T, PatinaError>`.
//!
//! All diagnostics use `syn::Error::new_spanned` so the compiler underlines
//! the offending token rather than the whole trait.

use proc_macro2::TokenStream;
use syn::{
    FnArg, GenericArgument, Ident, ItemTrait, Pat, PathArguments, Receiver, ReturnType,
    TraitItem, TraitItemFn, Type, Visibility, parse2,
};

/// A validated `#[service]` trait, ready for code generation.
pub struct ServiceTrait {
    pub vis: Visibility,
    pub attrs: Vec<syn::Attribute>,
    pub supertraits: syn::punctuated::Punctuated<syn::TypeParamBound, syn::Token![+]>,
    pub ident: Ident,
    pub methods: Vec<Method>,
}

/// A single RPC method extracted from the trait.
pub struct Method {
    pub ident: Ident,
    pub args: Vec<MethodArg>,
    /// The `T` in `Result<T, PatinaError>`.
    pub ok_type: Type,
}

pub struct MethodArg {
    pub name: Ident,
    pub ty: Type,
}

impl ServiceTrait {
    pub fn parse(input: TokenStream) -> syn::Result<Self> {
        let item: ItemTrait = match parse2::<ItemTrait>(input) {
            Ok(item) => item,
            Err(e) => {
                return Err(syn::Error::new(
                    e.span(),
                    "#[patina::service] may only be applied to a trait",
                ));
            }
        };

        if !item.generics.params.is_empty() {
            return Err(syn::Error::new_spanned(
                &item.generics,
                "#[patina::service] traits cannot be generic",
            ));
        }

        let mut methods = Vec::new();
        for trait_item in &item.items {
            match trait_item {
                TraitItem::Fn(method) => methods.push(Method::parse(method)?),
                other => {
                    return Err(syn::Error::new_spanned(
                        other,
                        "#[patina::service] traits may only contain methods",
                    ));
                }
            }
        }

        Ok(ServiceTrait {
            vis: item.vis.clone(),
            attrs: item.attrs.clone(),
            supertraits: item.supertraits.clone(),
            ident: item.ident.clone(),
            methods,
        })
    }
}

impl Method {
    fn parse(method: &TraitItemFn) -> syn::Result<Self> {
        let sig = &method.sig;

        if sig.asyncness.is_none() {
            return Err(syn::Error::new_spanned(
                sig.fn_token,
                "service methods must be `async`",
            ));
        }
        if method.default.is_some() {
            return Err(syn::Error::new_spanned(
                method,
                "service methods cannot have a default body",
            ));
        }
        if !sig.generics.params.is_empty() {
            return Err(syn::Error::new_spanned(
                &sig.generics,
                "service methods cannot be generic",
            ));
        }

        let mut inputs = sig.inputs.iter();
        match inputs.next() {
            Some(FnArg::Receiver(Receiver { reference: Some(_), mutability: None, .. })) => {}
            Some(FnArg::Receiver(receiver)) => {
                return Err(syn::Error::new_spanned(
                    receiver,
                    "service methods must take `&self` (not `&mut self` or `self`)",
                ));
            }
            _ => {
                return Err(syn::Error::new_spanned(
                    sig,
                    "service methods must take `&self` as the first argument",
                ));
            }
        }

        let mut args = Vec::new();
        for input in inputs {
            match input {
                FnArg::Typed(pat_type) => {
                    let name = match &*pat_type.pat {
                        Pat::Ident(pat_ident) => pat_ident.ident.clone(),
                        other => {
                            return Err(syn::Error::new_spanned(
                                other,
                                "service method arguments must be simple identifiers",
                            ));
                        }
                    };
                    args.push(MethodArg { name, ty: (*pat_type.ty).clone() });
                }
                FnArg::Receiver(receiver) => {
                    return Err(syn::Error::new_spanned(
                        receiver,
                        "unexpected `self` after the first argument",
                    ));
                }
            }
        }

        let ok_type = parse_result_ok(&sig.output)?;

        Ok(Method { ident: sig.ident.clone(), args, ok_type })
    }
}

/// Extract `T` from a `Result<T, PatinaError>` return type, rejecting anything
/// that isn't shaped exactly like that.
fn parse_result_ok(output: &ReturnType) -> syn::Result<Type> {
    let err = |tokens: &dyn quote::ToTokens| {
        syn::Error::new_spanned(tokens, "service methods must return `Result<T, PatinaError>`")
    };

    let ReturnType::Type(_, ty) = output else {
        return Err(syn::Error::new_spanned(
            output,
            "service methods must return `Result<T, PatinaError>`",
        ));
    };

    let Type::Path(type_path) = ty.as_ref() else {
        return Err(err(ty));
    };
    let last = type_path.path.segments.last().ok_or_else(|| err(ty))?;
    if last.ident != "Result" {
        return Err(err(&last.ident));
    }

    let PathArguments::AngleBracketed(generics) = &last.arguments else {
        return Err(err(last));
    };
    let type_args: Vec<&Type> = generics
        .args
        .iter()
        .filter_map(|arg| match arg {
            GenericArgument::Type(t) => Some(t),
            _ => None,
        })
        .collect();
    if type_args.len() != 2 {
        return Err(err(generics));
    }

    let is_patina_error = matches!(
        type_args[1],
        Type::Path(p) if p.path.segments.last().is_some_and(|s| s.ident == "PatinaError")
    );
    if !is_patina_error {
        return Err(err(type_args[1]));
    }

    Ok(type_args[0].clone())
}
