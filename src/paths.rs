pub fn make_path(parent: &str, child: &str) -> String {
    let mut result = parent.to_string();
    if parent.chars().last().unwrap() != '/' {
        result.push('/');
    }
    result.push_str(child);
    result
}
