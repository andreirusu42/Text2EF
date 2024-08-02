use std::collections::HashSet;
use std::hash::Hash;

#[derive(Debug)]
pub struct CaseInsensitiveHashSet<T> {
    set: HashSet<T>,
    lowercase_set: HashSet<String>,
}

impl<T: AsRef<str> + Eq + Hash + Clone> CaseInsensitiveHashSet<T> {
    pub fn new() -> Self {
        CaseInsensitiveHashSet {
            set: HashSet::new(),
            lowercase_set: HashSet::new(),
        }
    }

    pub fn insert(&mut self, value: T) -> bool {
        let lowercase_value = value.as_ref().to_lowercase();
        if self.lowercase_set.insert(lowercase_value) {
            self.set.insert(value);
            true
        } else {
            false
        }
    }

    pub fn contains(&self, value: &T) -> bool {
        let lowercase_value = value.as_ref().to_lowercase();
        self.lowercase_set.contains(&lowercase_value)
    }

    pub fn remove(&mut self, value: &T) -> bool {
        let lowercase_value = value.as_ref().to_lowercase();
        if self.lowercase_set.remove(&lowercase_value) {
            let original_value = self
                .set
                .iter()
                .find(|&s| s.as_ref().to_lowercase() == lowercase_value)
                .cloned();
            if let Some(value_to_remove) = original_value {
                self.set.remove(&value_to_remove);
            }
            true
        } else {
            false
        }
    }

    pub fn len(&self) -> usize {
        self.set.len()
    }

    pub fn is_empty(&self) -> bool {
        self.set.is_empty()
    }

    pub fn clear(&mut self) {
        self.set.clear();
        self.lowercase_set.clear();
    }

    pub fn iter(&self) -> impl Iterator<Item = &T> {
        self.set.iter()
    }
}

impl<'a, T: AsRef<str> + Eq + Hash> IntoIterator for &'a CaseInsensitiveHashSet<T> {
    type Item = &'a T;
    type IntoIter = std::collections::hash_set::Iter<'a, T>;

    fn into_iter(self) -> Self::IntoIter {
        self.set.iter()
    }
}

impl<T: AsRef<str> + Eq + Hash> IntoIterator for CaseInsensitiveHashSet<T> {
    type Item = T;
    type IntoIter = std::collections::hash_set::IntoIter<T>;

    fn into_iter(self) -> Self::IntoIter {
        self.set.into_iter()
    }
}
