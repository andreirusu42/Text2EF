use std::collections::{HashMap, HashSet, VecDeque};

pub fn determine_join_order(
    join_constraints: &mut Vec<Vec<(&str, &str)>>,
    main_table_alias: &str,
) -> Vec<String> {
    let mut seen: HashSet<&str> = HashSet::new();
    let mut order: Vec<String> = Vec::new();

    seen.insert(main_table_alias);
    order.push(main_table_alias.to_string());

    for i in 0..join_constraints.len() {
        let constraints = &mut join_constraints[i];

        while constraints.len() > 0 {
            let mut constraints_to_remove: Vec<(&str, &str)> = Vec::new();

            for constrain in constraints.iter() {
                if seen.contains(&constrain.0) || seen.contains(&constrain.1) {
                    if seen.insert(constrain.0) {
                        order.push(constrain.0.to_string());
                    }

                    if seen.insert(constrain.1) {
                        order.push(constrain.1.to_string());
                    }

                    constraints_to_remove.push(constrain.clone());
                }
            }

            for constraint in constraints_to_remove {
                constraints.retain(|c| *c != constraint);
            }
        }
    }

    order
}
