use std::collections::{HashMap, HashSet, VecDeque};

pub fn determine_join_order(
    join_constraints: &mut Vec<Vec<(String, String)>>,
    all_tables: Vec<String>,
    main_table_alias: String,
    m2m_table_alias: Option<&String>,
) -> Vec<String> {
    let mut seen: HashSet<String> = HashSet::new();
    let mut order: Vec<String> = Vec::new();

    seen.insert(main_table_alias.clone());

    if let Some(m2m_table_alias) = m2m_table_alias {
        let lower = m2m_table_alias.to_lowercase();
        seen.insert(lower.clone());
    }

    order.push(main_table_alias);

    for i in 0..join_constraints.len() {
        let constraints = &mut join_constraints[i];

        while !constraints.is_empty() {
            let mut constraints_to_remove: Vec<(String, String)> = Vec::new();

            for constrain in constraints.iter() {
                if seen.contains(&constrain.0) || seen.contains(&constrain.1) {
                    if seen.insert(constrain.0.clone()) {
                        order.push(constrain.0.clone());
                    }

                    if seen.insert(constrain.1.clone()) {
                        order.push(constrain.1.clone());
                    }

                    constraints_to_remove.push(constrain.clone());
                }
            }

            for constraint in constraints_to_remove {
                constraints.retain(|c| *c != constraint);
            }
        }
    }

    for table in all_tables.iter() {
        if !seen.contains(table) {
            order.push(table.clone());
        }
    }

    order
}
