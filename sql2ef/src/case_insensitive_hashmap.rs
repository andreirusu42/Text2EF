use std::collections::HashMap;
use std::hash::Hash;

#[derive(Debug)]
pub struct CaseInsensitiveHashMap<K, V> {
    map: HashMap<K, V>,
    lowercase_map: HashMap<String, K>,
}

impl<K: AsRef<str> + Eq + Hash + Clone, V> CaseInsensitiveHashMap<K, V> {
    pub fn new() -> Self {
        CaseInsensitiveHashMap {
            map: HashMap::new(),
            lowercase_map: HashMap::new(),
        }
    }

    pub fn insert(&mut self, key: K, value: V) -> Option<V> {
        let lowercase_key = key.as_ref().to_lowercase();

        if let Some(existing_key) = self.lowercase_map.get(&lowercase_key) {
            self.map.insert(existing_key.clone(), value)
        } else {
            self.lowercase_map.insert(lowercase_key, key.clone());
            self.map.insert(key, value)
        }
    }

    pub fn get(&self, key: &K) -> Option<&V> {
        let lowercase_key = key.as_ref().to_lowercase();
        self.lowercase_map
            .get(&lowercase_key)
            .and_then(|original_key| self.map.get(original_key))
    }

    pub fn remove(&mut self, key: &K) -> Option<V> {
        let lowercase_key = key.as_ref().to_lowercase();
        if let Some(original_key) = self.lowercase_map.remove(&lowercase_key) {
            self.map.remove(&original_key)
        } else {
            None
        }
    }

    pub fn contains_key(&self, key: &K) -> bool {
        let lowercase_key = key.as_ref().to_lowercase();
        self.lowercase_map.contains_key(&lowercase_key)
    }

    pub fn len(&self) -> usize {
        self.map.len()
    }

    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
    }

    pub fn clear(&mut self) {
        self.map.clear();
        self.lowercase_map.clear();
    }

    pub fn iter(&self) -> impl Iterator<Item = (&K, &V)> {
        self.map.iter()
    }
}

impl<'a, K: AsRef<str> + Eq + Hash, V> IntoIterator for &'a CaseInsensitiveHashMap<K, V> {
    type Item = (&'a K, &'a V);
    type IntoIter = std::collections::hash_map::Iter<'a, K, V>;

    fn into_iter(self) -> Self::IntoIter {
        self.map.iter()
    }
}

impl<K: AsRef<str> + Eq + Hash, V> IntoIterator for CaseInsensitiveHashMap<K, V> {
    type Item = (K, V);
    type IntoIter = std::collections::hash_map::IntoIter<K, V>;

    fn into_iter(self) -> Self::IntoIter {
        self.map.into_iter()
    }
}
