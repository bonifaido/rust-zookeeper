use std::fmt;
use std::ops;

use std::string::ToString;

/// Describes the ability of a user to perform a certain action.
///
/// Permissions can be mixed together like integers with `|` and `&`.
#[derive(Clone, Copy, Debug, PartialEq)]
#[repr(u32)]
pub enum Permission {
    /// No permissions are set (server could have been configured without ACL support).
    None =   0b00000,
    /// You can access the data of a node and can list its children.
    Read =   0b00001,
    /// You can set the data of a node.
    Write =  0b00010,
    /// You can create a child node.
    Create = 0b00100,
    /// You can delete a child node (but not necessarily this one).
    Delete = 0b01000,
    /// You can alter permissions on this node.
    Admin =  0b10000,
    /// You can do anything.
    All =    0b11111,
}

impl Permission {
    /// Extract a permission value from raw `bits`.
    pub fn from_raw(bits: u32) -> Permission {
        use std::mem;

        unsafe { mem::transmute(bits) }
    }

    /// Check that all `permissions` are set.
    ///
    /// ```
    /// use zookeeper::Permission;
    ///
    /// (Permission::Read | Permission::Write).can(Permission::Write); // -> true
    /// Permission::Admin.can(Permission::Create); // -> false
    /// ```
    pub fn can(self, permissions: Permission) -> bool {
        (self & permissions) == permissions
    }
}

impl ops::BitAnd for Permission {
    type Output = Self;

    fn bitand(self, rhs: Self) -> Self {
        Permission::from_raw((self as u32) & (rhs as u32))
    }
}

impl ops::BitOr for Permission {
    type Output = Self;

    fn bitor(self, rhs: Self) -> Self {
        Permission::from_raw((self as u32) | (rhs as u32))
    }
}

impl ops::Not for Permission {
    type Output = Self;

    fn not(self) -> Self {
        Permission::from_raw(!(self as u32)) & Permission::All
    }
}

impl fmt::Display for Permission {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        if *self == Permission::All {
            write!(f, "All")
        } else if *self == Permission::None {
            write!(f, "None")
        } else {
            let mut first = true;
            let mut tick = || {
                if first {
                    first = false;
                    ""
                } else {
                    "|"
                }
            };

            if self.can(Permission::Read)   { write!(f, "{}Read",   tick())?; }
            if self.can(Permission::Write)  { write!(f, "{}Write",  tick())?; }
            if self.can(Permission::Create) { write!(f, "{}Create", tick())?; }
            if self.can(Permission::Delete) { write!(f, "{}Delete", tick())?; }
            if self.can(Permission::Admin)  { write!(f, "{}Admin",  tick())?; }
            Ok(())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn permission_bitor() {
        let all = Permission::Read
                | Permission::Write
                | Permission::Create
                | Permission::Delete
                | Permission::Admin;
        assert_eq!(Permission::All, all);
    }

    #[test]
    fn permission_can() {
        assert!(Permission::All.can(Permission::Write));
        assert!(!Permission::Write.can(Permission::Read));
    }

    #[test]
    fn permission_format() {
        assert_eq!("All", Permission::All.to_string());
        assert_eq!("None", Permission::None.to_string());
        assert_eq!("Read|Write", (Permission::Read | Permission::Write).to_string());
        assert_eq!("Create|Delete", (Permission::Create | Permission::Delete).to_string());
        assert_eq!("Admin", Permission::Admin.to_string());
    }
}

/// An access control list.
///
/// In general, the ACL system is similar to UNIX file access permissions, where znodes act as
/// files. Unlike UNIX, each znode can have any number of ACLs to correspond with the potentially
/// limitless (and pluggable) authentication schemes. A more surprising difference is that ACLs are
/// not recursive: If `/path` is only readable by a single user, but `/path/sub` is world-readable,
/// then anyone will be able to read `/path/sub`.
///
/// See the [ZooKeeper Programmer's Guide](https://zookeeper.apache.org/doc/trunk/zookeeperProgrammers.html#sc_ZooKeeperAccessControl)
/// for more information.
#[derive(Clone, Debug, PartialEq)]
pub struct Acl {
    /// The permissions associated with this ACL.
    pub perms: Permission,
    /// The authentication scheme this list is used for. The most common scheme is `"auth"`, which
    /// allows any authenticated user to do anything (see `creator_all`).
    pub scheme: String,
    /// The ID of the user under the `scheme`. For example, with the `"ip"` `scheme`, this is an IP
    /// address or CIDR netmask.
    pub id: String,
}

impl Acl {
    /// Create a new ACL with the given `permissions`, `scheme`, and `id`.
    pub fn new<T, U>(permissions: Permission, scheme: T, id: U) -> Acl
            where T: ToString, U: ToString {
        Acl {
            perms: permissions,
            scheme: scheme.to_string(),
            id: id.to_string(),
        }
    }

    /// This ACL gives the creators authentication id's all permissions.
    pub fn creator_all() -> &'static Vec<Acl> {
        &ACL_CREATOR_ALL
    }

    /// This is a completely open ACL.
    pub fn open_unsafe() -> &'static Vec<Acl> {
        &ACL_OPEN_UNSAFE
    }

    /// This ACL gives the world the ability to read.
    pub fn read_unsafe() -> &'static Vec<Acl> {
        &ACL_READ_UNSAFE
    }
}

lazy_static! {
    static ref ACL_CREATOR_ALL: Vec<Acl> = vec![Acl::new(Permission::All, "auth", "")];
    static ref ACL_OPEN_UNSAFE: Vec<Acl> = vec![Acl::new(Permission::All, "world", "anyone")];
    static ref ACL_READ_UNSAFE: Vec<Acl> = vec![Acl::new(Permission::Read, "world", "anyone")];
}

impl fmt::Display for Acl {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "({}:{}, {})", self.scheme, self.id, self.perms)
    }
}
