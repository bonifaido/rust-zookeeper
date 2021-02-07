/// Combine two paths into a single path, possibly inserting a '/' between them.
pub fn make_path(parent: &str, child: &str) -> String {
    if parent.chars().last() == Some('/') {
        format!("{}{}", parent, child)
    } else {
        format!("{}/{}", parent, child)
    }
}

#[cfg(test)]
#[test]
fn make_path_tests() {
    assert_eq!("/a/b", make_path("/a", "b"));
    assert_eq!("/a/b", make_path("/a/", "b"));
}
