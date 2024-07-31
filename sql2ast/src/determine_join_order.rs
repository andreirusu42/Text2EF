use std::collections::{HashMap, HashSet, VecDeque};

pub fn determine_join_order(
    dependencies: Vec<(&str, &str)>,
    main_table_alias: &str,
) -> Vec<String> {
    let tables = dependencies
        .iter()
        .flat_map(|&(src, dest)| vec![src, dest])
        .collect::<HashSet<&str>>();

    let mut graph: HashMap<&str, Vec<&str>> = HashMap::new();
    let mut in_degree: HashMap<&str, i32> = HashMap::new();

    for &(src, dest) in &dependencies {
        graph.entry(src).or_default().push(dest);
        *in_degree.entry(dest).or_default() += 1;
    }

    for &table in &tables {
        in_degree.entry(table).or_insert(0);
    }

    let mut order = Vec::new();
    let mut queue: VecDeque<&str> = VecDeque::new();
    let mut visited: HashSet<&str> = HashSet::new();

    queue.push_back(main_table_alias);

    while let Some(current) = queue.pop_front() {
        order.push(current.to_string());
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

    let remaining_tables: Vec<&str> = tables.difference(&visited).cloned().collect();
    let mut remaining_tables = remaining_tables;

    while !remaining_tables.is_empty() {
        let mut found = false;
        for i in 0..remaining_tables.len() {
            let table = remaining_tables[i];
            if graph
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
