#![crate_type = "proc-macro"]

extern crate syn;
#[macro_use]
extern crate quote;
extern crate proc_macro;

use proc_macro::TokenStream;
use std::iter::Iterator;

/// Emit an `std::fmt::Display` implementation for an enum type.
#[proc_macro_derive(EnumDisplay)]
pub fn emit_enum_display(input: TokenStream) -> TokenStream {
    let ast: syn::DeriveInput = syn::parse(input).unwrap();

    let name = &ast.ident;

    let toks = quote! {
        impl ::std::fmt::Display for #name {
            fn fmt(&self, f: &mut ::std::fmt::Formatter) -> ::std::fmt::Result {
                write!(f, "{:?}", self)
            }
        }
    };

    toks.into()
}

/// Emit an `std::error::Error` implementation for an enum type. This is most useful in conjunction
/// with `Debug` and `EnumDisplay`.
#[proc_macro_derive(EnumError)]
pub fn emit_enum_error(input: TokenStream) -> TokenStream {
    let ast: syn::DeriveInput = syn::parse(input).unwrap();

    let name = &ast.ident;

    let en = match ast.data {
        syn::Data::Enum(ref v) => v,
        _ => panic!("EnumError only works for enum types"),
    };

    let mut serializations = Vec::new();

    for variant in &en.variants {
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

    toks.into()
}

/// Emit an `std::convert::From<i32>` implementation for an enum type. When converting from an `i32`
/// that does not have an enumeration value, the system will panic, unless a fallback enumeration
/// symbol is specified with `#[EnumConvertFromIntFallback = "Identifier"]`.
#[proc_macro_derive(EnumConvertFromInt, attributes(EnumConvertFromIntFallback))]
pub fn emit_enum_from_primitive(input: TokenStream) -> TokenStream {
    let ast: syn::DeriveInput = syn::parse(input).unwrap();

    let name = &ast.ident;

    let en = match ast.data {
        syn::Data::Enum(ref v) => v,
        _ => panic!("EnumConvertFromInt only works for enum types"),
    };

    let mut serializations = Vec::new();

    for variant in &en.variants {
        let ident = &variant.ident;
        let val = &match variant.discriminant {
            Some(ref expr) => &expr.1,
            None => panic!("{}::{} does not have an associated value", name, ident),
        };
        serializations.push(quote! {
            #val => #name::#ident
        });
    }

    let is_fallback_value = |attr: &syn::Attribute| match attr.interpret_meta() {
        Some(syn::Meta::NameValue(syn::MetaNameValue { ident, .. })) => {
            ident == "EnumConvertFromIntFallback"
        }
        _ => false,
    };
    let fallback = match ast.attrs.into_iter().find(&is_fallback_value) {
        Some(attr) => match attr.interpret_meta() {
            Some(syn::Meta::NameValue(syn::MetaNameValue {
                lit: syn::Lit::Str(val),
                ..
            })) => {
                let id = syn::Ident::new(&val.value(), val.span());
                quote! { #name::#id }
            }
            _ => panic!("EnumConvertFromIntFallback must be a string"),
        },
        None => quote! { panic!("Received unexpected value {}", val) },
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

    toks.into()
}
