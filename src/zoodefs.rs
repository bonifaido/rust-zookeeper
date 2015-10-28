pub mod perms {
    pub const READ: i32 = 1;
    pub const WRITE: i32 = 1 << 1;
    pub const CREATE: i32 = 1 << 2;
    pub const DELETE: i32 = 1 << 3;
    pub const ADMIN: i32 = 1 << 4;
    pub const ALL: i32 = READ | WRITE | CREATE | DELETE | ADMIN;
}

pub mod acls {
    use ::perms;
    use proto::Acl;

    fn acl(perm: i32, scheme: &str, id: &str) -> Vec<Acl> {
        vec![Acl{perms: perm, scheme: scheme.to_owned(), id: id.to_owned()}]
    }

    lazy_static!{
        pub static ref CREATOR_ALL_ACL: Vec<Acl> = acl(perms::ALL, "auth", "");
        pub static ref OPEN_ACL_UNSAFE: Vec<Acl> = acl(perms::ALL, "world", "anyone");
        pub static ref READ_ACL_UNSAFE: Vec<Acl> = acl(perms::READ, "world", "anyone");
    }
}
