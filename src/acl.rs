use std::fmt;
use std::ops;

use std::string::ToString;

use lazy_static::lazy_static;

/// Describes the ability of a user to perform a certain action.
///
/// Permissions can be mixed together like integers with `|` and `&`.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct Permission(u32);

impl Permission {
    /// No permissions are set (server could have been configured without ACL support).
    pub const NONE: Permission = Permission(0b00000);

    /// You can access the data of a node and can list its children.
    pub const READ: Permission = Permission(0b00001);

    /// You can set the data of a node.
    pub const WRITE: Permission = Permission(0b00010);

    /// You can create a child node.
    pub const CREATE: Permission = Permission(0b00100);

    /// You can delete a child node (but not necessarily this one).
    pub const DELETE: Permission = Permission(0b01000);

    /// You can alter permissions on this node.
    pub const ADMIN: Permission = Permission(0b10000);

    /// You can do anything.
    pub const ALL: Permission = Permission(0b11111);

    /// Extract a permission value from raw `bits`.
    pub(crate) fn from_raw(bits: u32) -> Permission {
        Permission(bits)
    }

    pub(crate) fn code(&self) -> u32 {
        self.0
    }

    /// Check that all `permissions` are set.
    ///
    /// ```
    /// use zookeeper_async::Permission;
    ///
    /// (Permission::READ | Permission::WRITE).can(Permission::WRITE); // -> true
    /// Permission::ADMIN.can(Permission::CREATE); // -> false
    /// ```
    pub fn can(self, permissions: Permission) -> bool {
        (self & permissions) == permissions
    }
}

impl ops::BitAnd for Permission {
    type Output = Self;

    fn bitand(self, rhs: Self) -> Self {
        Permission::from_raw(self.0 & rhs.0)
    }
}

impl ops::BitOr for Permission {
    type Output = Self;

    fn bitor(self, rhs: Self) -> Self {
        Permission::from_raw(self.0 | rhs.0)
    }
}

impl fmt::Display for Permission {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        if *self == Permission::ALL {
            write!(f, "ALL")
        } else if *self == Permission::NONE {
            write!(f, "NONE")
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

            if self.can(Permission::READ) {
                write!(f, "{}READ", tick())?;
            }
            if self.can(Permission::WRITE) {
                write!(f, "{}WRITE", tick())?;
            }
            if self.can(Permission::CREATE) {
                write!(f, "{}CREATE", tick())?;
            }
            if self.can(Permission::DELETE) {
                write!(f, "{}DELETE", tick())?;
            }
            if self.can(Permission::ADMIN) {
                write!(f, "{}ADMIN", tick())?;
            }
            Ok(())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn permission_bitor() {
        let all = Permission::READ
            | Permission::WRITE
            | Permission::CREATE
            | Permission::DELETE
            | Permission::ADMIN;
        assert_eq!(Permission::ALL, all);
    }

    #[test]
    fn permission_can() {
        assert!(Permission::ALL.can(Permission::WRITE));
        assert!(!Permission::WRITE.can(Permission::READ));
    }

    #[test]
    fn permission_format() {
        assert_eq!("ALL", Permission::ALL.to_string());
        assert_eq!("NONE", Permission::NONE.to_string());
        assert_eq!(
            "READ|WRITE",
            (Permission::READ | Permission::WRITE).to_string()
        );
        assert_eq!(
            "CREATE|DELETE",
            (Permission::CREATE | Permission::DELETE).to_string()
        );
        assert_eq!("ADMIN", Permission::ADMIN.to_string());
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
    where
        T: ToString,
        U: ToString,
    {
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
    static ref ACL_CREATOR_ALL: Vec<Acl> = vec![Acl::new(Permission::ALL, "auth", "")];
    static ref ACL_OPEN_UNSAFE: Vec<Acl> = vec![Acl::new(Permission::ALL, "world", "anyone")];
    static ref ACL_READ_UNSAFE: Vec<Acl> = vec![Acl::new(Permission::READ, "world", "anyone")];
}

impl fmt::Display for Acl {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "({}:{}, {})", self.scheme, self.id, self.perms)
    }
}
