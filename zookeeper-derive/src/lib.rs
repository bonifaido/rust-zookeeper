#![crate_type="proc-macro"]

extern crate syn;
#[macro_use]
extern crate quote;
extern crate proc_macro;

use proc_macro::TokenStream;
use std::iter::Iterator;

/// Emit an `std::fmt::Display` implementation for an enum type.
#[proc_macro_derive(EnumDisplay)]
pub fn emit_enum_display(input: TokenStream) -> TokenStream {
    let s = input.to_string();
    let ast = syn::parse_derive_input(&s).unwrap();

    let name = &ast.ident;

    let toks = quote! {
        impl ::std::fmt::Display for #name {
            fn fmt(&self, f: &mut ::std::fmt::Formatter) -> ::std::fmt::Result {
                write!(f, "{:?}", self)
            }
        }
    };

    toks.parse().unwrap()
}

/// Emit an `std::error::Error` implementation for an enum type. This is most useful in conjunction
/// with `Debug` and `EnumDisplay`.
#[proc_macro_derive(EnumError)]
pub fn emit_enum_error(input: TokenStream) -> TokenStream {
    let s = input.to_string();
    let ast = syn::parse_derive_input(&s).unwrap();

    let name = &ast.ident;

    let variants = match ast.body {
        syn::Body::Enum(ref v) => v,
        _                      => panic!("EnumError only works for enum types")
    };

    let mut serializations = Vec::new();

    for variant in variants {
        let ident = &variant.ident;
        let ident_str = format!("{}", ident);
        serializations.push(quote! {
            &#name::#ident => #ident_str
        });
    }

    let toks = quote! {
        impl ::std::error::Error for #name {
            fn description(&self) -> &str {
                match self {
                    #(#serializations),*
                }
            }
        }
    };

    toks.parse().unwrap()
}

/// Emit an `std::convert::From<i32>` implementation for an enum type. When converting from an `i32`
/// that does not have an enumeration value, the system will panic, unless a fallback enumeration
/// symbol is specified with `#[EnumConvertFromIntFallback = "Identifier"]`.
#[proc_macro_derive(EnumConvertFromInt, attributes(EnumConvertFromIntFallback))]
pub fn emit_enum_from_primitive(input: TokenStream) -> TokenStream {
    let s = input.to_string();
    let ast = syn::parse_derive_input(&s).unwrap();

    let name = &ast.ident;

    let variants = match ast.body {
        syn::Body::Enum(ref v) => v,
        _                      => panic!("EnumConvertFromInt only works for enum types")
    };

    let mut serializations = Vec::new();

    for variant in variants {
        let ident = &variant.ident;
        let val = &match variant.discriminant {
            Some(ref expr) => expr,
            None => panic!("{}::{} does not have an associated value", name, ident)
        };
        serializations.push(quote! {
            #val => #name::#ident
        });
    }

    let is_fallback_value = |attr: &syn::Attribute| {
        match &attr.value {
            &syn::MetaItem::NameValue(ref ident, _) => ident == "EnumConvertFromIntFallback",
            _ => false
        }
    };
    let fallback = match ast.attrs.iter().position(&is_fallback_value) {
        Some(idx) => {
            match &ast.attrs[idx].value {
                &syn::MetaItem::NameValue(_, syn::Lit::Str(ref val, _)) => {
                    let id = syn::Ident::new(val.as_str());
                    quote! { #name::#id }
                }
                _ => panic!("EnumConvertFromIntFallback must be a string")
            }
        },
        None => quote! { panic!("Received unexpected value {}", val) }
    };

    let toks = quote! {
        impl ::std::convert::From<i32> for #name {
            fn from(val: i32) -> #name {
                match val {
                    #(#serializations),*,
                    _ => #fallback
                }
            }
        }
    };

    toks.parse().unwrap()
}
