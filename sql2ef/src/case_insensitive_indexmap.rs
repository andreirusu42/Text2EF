use std::collections::HashMap;

use indexmap::IndexMap;

#[derive(Debug)]
pub struct CaseInsensitiveIndexMap<V> {
    map: IndexMap<String, V>,
    lowercase_map: IndexMap<String, String>,
}

impl<V> CaseInsensitiveIndexMap<V> {
    pub fn new() -> Self {
        CaseInsensitiveIndexMap {
            map: IndexMap::new(),
            lowercase_map: IndexMap::new(),
        }
    }

    pub fn insert<K: AsRef<str>>(&mut self, key: K, value: V) {
        let key_str = key.as_ref().to_string();
        let lowercase_key = key.as_ref().to_lowercase();
        self.lowercase_map.insert(lowercase_key, key_str.clone());
        self.map.insert(key_str, value);
    }

    pub fn get<K: AsRef<str>>(&self, key: K) -> Option<&V> {
        let lowercase_key = key.as_ref().to_lowercase();
        if let Some(original_key) = self.lowercase_map.get(&lowercase_key) {
            self.map.get(original_key)
        } else {
            None
        }
    }

    pub fn get_mut<K: AsRef<str>>(&mut self, key: K) -> Option<&mut V> {
        let lowercase_key = key.as_ref().to_lowercase();
        if let Some(original_key) = self.lowercase_map.get(&lowercase_key) {
            self.map.get_mut(original_key)
        } else {
            None
        }
    }

    pub fn contains_key<K: AsRef<str>>(&self, key: K) -> bool {
        let lowercase_key = key.as_ref().to_lowercase();
        self.lowercase_map.contains_key(&lowercase_key)
    }

    pub fn keys(&self) -> impl Iterator<Item = &String> {
        self.map.keys()
    }

    pub fn len(&self) -> usize {
        self.map.len()
    }
}

impl<'a, V> IntoIterator for &'a CaseInsensitiveIndexMap<V> {
    type Item = (&'a String, &'a V);
    type IntoIter = indexmap::map::Iter<'a, String, V>;

    fn into_iter(self) -> Self::IntoIter {
        self.map.iter()
    }
}

impl<V> IntoIterator for CaseInsensitiveIndexMap<V> {
    type Item = (String, V);
    type IntoIter = indexmap::map::IntoIter<String, V>;

    fn into_iter(self) -> Self::IntoIter {
        self.map.into_iter()
    }
}
