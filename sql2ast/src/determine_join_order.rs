use std::collections::{HashMap, HashSet, VecDeque};

pub fn determine_join_order(
    join_constraints: Vec<Vec<(&str, &str)>>,
    main_table_alias: &str,
) -> Vec<String> {
    let mut order = Vec::new();
    let mut visited: HashSet<&str> = HashSet::new();
    let mut full_graph: HashMap<&str, Vec<&str>> = HashMap::new();

    for constraints in &join_constraints {
        let mut graph: HashMap<&str, Vec<&str>> = HashMap::new();
        let mut in_degree: HashMap<&str, i32> = HashMap::new();

        for &(src, dest) in constraints {
            graph.entry(src).or_default().push(dest);
            full_graph.entry(src).or_default().push(dest);

            *in_degree.entry(dest).or_default() += 1;
            in_degree.entry(src).or_insert(0);
        }

        let mut queue: VecDeque<&str> = VecDeque::new();

        queue.push_back(main_table_alias);

        while let Some(current) = queue.pop_front() {
            if !visited.contains(current) {
                order.push(current.to_string());
            }

            visited.insert(current);

            if let Some(neighbors) = graph.get(current) {
                for &neighbor in neighbors {
                    if let Some(in_deg) = in_degree.get_mut(neighbor) {
                        *in_deg -= 1;
                        if *in_deg == 0 {
                            queue.push_back(neighbor);
                        }
                    }
                }
            }
        }
    }

    let tables: HashSet<&str> = join_constraints
        .iter()
        .flat_map(|level| level.iter().flat_map(|&(src, dest)| vec![src, dest]))
        .collect();

    let remaining_tables: Vec<&str> = tables.difference(&visited).cloned().collect();
    let mut remaining_tables = remaining_tables;

    while !remaining_tables.is_empty() {
        let mut found = false;
        for i in 0..remaining_tables.len() {
            let table = remaining_tables[i];
            if full_graph
                .get(table)
                .unwrap_or(&vec![])
                .iter()
                .all(|&dep| order.contains(&dep.to_string()))
            {
                order.push(table.to_string());
                remaining_tables.remove(i);
                found = true;
                break;
            }
        }
        if !found {
            break;
        }
    }

    order
}
