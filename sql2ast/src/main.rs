use std::collections::HashMap;
use std::os::macos::raw;

use sqlparser::ast::{
    BinaryOperator, Expr, GroupByExpr, Query, Select, SelectItem, SetExpr, Statement,
    TableWithJoins,
};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;

mod schema_mapping;

use schema_mapping::{create_schema_map, SchemaMapping};

pub struct LinqQueryBuilder {
    schema_mapping: SchemaMapping,
    row_selector: String,
    group_selector: String,
}

impl LinqQueryBuilder {
    pub fn new(model_folder_path: &str) -> Self {
        let schema_mapping = create_schema_map(model_folder_path);

        LinqQueryBuilder {
            schema_mapping,
            row_selector: "row".to_string(),
            group_selector: "group".to_string(),
        }
    }

    fn build_projection(
        &self,
        select: &Box<Select>,
        tables_with_aliases_map: &HashMap<String, String>,
        main_table_name: &str,
        has_group_by: bool,
        group_by_fields: Vec<String>,
    ) -> String {
        let mut result = String::new();
        let selector = if has_group_by {
            &self.group_selector
        } else {
            &self.row_selector
        };

        result.push_str(&format!(".Select({} => new {{ ", selector));

        let mut select_fields: Vec<String> = Vec::new();
        for select_item in &select.projection {
            if let SelectItem::UnnamedExpr(expr) = select_item {
                if let Expr::Identifier(identifier) = expr {
                    let column_name = identifier.to_string();
                    let table_alias = self
                        .get_table_alias_from_field_name(&tables_with_aliases_map, &column_name);

                    let mapped_column_name = self
                        .schema_mapping
                        .get_column_name(&main_table_name, &column_name)
                        .unwrap();

                    if table_alias.is_empty() {
                        if has_group_by {
                            if group_by_fields.contains(&mapped_column_name) {
                                select_fields
                                    .push(format!("{}.Key.{}", selector, mapped_column_name));
                            } else {
                                select_fields.push(format!("{}.{}", selector, mapped_column_name));
                            }
                        } else {
                            select_fields.push(format!("{}.{}", selector, mapped_column_name));
                        }
                    } else {
                        select_fields.push(format!(
                            "{}.{}.{}",
                            selector, table_alias, mapped_column_name
                        ));
                    }
                } else if let Expr::CompoundIdentifier(identifier) = expr {
                    let table_alias = identifier[0].to_string();
                    let column_name = identifier[1].to_string();

                    let mapped_column_name = self
                        .schema_mapping
                        .get_column_name(&main_table_name, &column_name)
                        .unwrap();

                    if has_group_by {
                        if group_by_fields.contains(&mapped_column_name) {
                            select_fields.push(format!("{}.Key.{}", selector, mapped_column_name));
                        } else {
                            select_fields
                                .push(format!("{}.First().{}", selector, mapped_column_name));
                        }
                    } else {
                        select_fields.push(format!(
                            "{}.{}.{}",
                            selector, table_alias, mapped_column_name
                        ));
                    }
                } else if let Expr::Function(function) = expr {
                    if function.name.to_string().to_lowercase() == "count" {
                        select_fields.push(format!("Count = {}.Count()", selector));
                    } else {
                        panic!("Unknown function");
                    }
                } else {
                    panic!("Unknown expression type");
                }
            } else {
                panic!("Unknown select item type");
            }
        }
        result.push_str(&select_fields.join(", "));
        result.push_str(" })");

        return result;
    }

    /*
       In Entity Framework, the behavior is stricter and more akin to standard SQL rules which require you to include all non-aggregated columns in the GROUP BY clause.
    */
    fn build_group_by(&self, select: &Box<Select>, table_name: &str) -> (String, Vec<String>) {
        let mut group_by_fields: Vec<String> = Vec::new();
        let mut raw_group_by_fields: Vec<String> = Vec::new();

        if let GroupByExpr::Expressions(expressions) = &select.group_by {
            let mut result = format!(".GroupBy({} => new {{ ", self.row_selector);

            if expressions.len() == 0 {
                return (String::new(), Vec::new());
            }

            for expr in expressions {
                if let Expr::Identifier(ident) = expr {
                    let column_name = ident.to_string();
                    let mapped_column_name = self
                        .schema_mapping
                        .get_column_name(&table_name, &column_name)
                        .unwrap();

                    group_by_fields.push(format!("{}.{}", self.row_selector, mapped_column_name));
                    raw_group_by_fields.push(mapped_column_name.to_string());
                } else if let Expr::CompoundIdentifier(identifiers) = expr {
                    let table_alias = identifiers[0].to_string();
                    let column_name = identifiers[1].to_string();

                    let mapped_column_name = self
                        .schema_mapping
                        .get_column_name(&table_name, &column_name)
                        .unwrap();

                    group_by_fields.push(format!(
                        "{}.{}.{}",
                        self.row_selector, table_alias, mapped_column_name
                    ));
                    raw_group_by_fields.push(mapped_column_name.to_string());
                } else {
                    panic!("Unknown expression type");
                }
            }

            result.push_str(&group_by_fields.join(", "));
            result.push_str(" })");

            return (result, raw_group_by_fields);
        }

        return (String::new(), Vec::new());
    }

    fn get_table_alias_from_field_name<'a>(
        &'a self,
        tables_with_aliases_map: &'a HashMap<String, String>,
        field_name: &str,
    ) -> &String {
        let mut result: Option<&String> = None;

        for (table_name, table_alias) in tables_with_aliases_map {
            let columns = self.schema_mapping.get_table_columns(table_name).unwrap();

            if columns.contains_key(field_name) {
                if result.is_some() {
                    panic!("Ambiguous field name");
                }
                result = Some(table_alias);
            }
        }

        result.expect("Field name not found")
    }

    fn build_where(
        &self,
        select: &Box<Select>,
        tables_with_aliases_map: &HashMap<String, String>,
        main_table_name: &str,
    ) -> String {
        let mut current_query = format!(".Where({} => ", self.row_selector);

        if let Some(selection) = &select.selection {
            let where_clause = self.build_where_helper(selection, tables_with_aliases_map, None);
            current_query.push_str(&where_clause);
        }

        current_query.push_str(")");

        return current_query;
    }

    fn build_where_helper(
        &self,
        expr: &Expr,
        tables_with_aliases_map: &HashMap<String, String>,
        parent_precedence: Option<i32>,
    ) -> String {
        match expr {
            Expr::BinaryOp { left, op, right } => match op {
                BinaryOperator::And | BinaryOperator::Or => {
                    let precedence = match op {
                        BinaryOperator::And => 1,
                        BinaryOperator::Or => 0,
                        _ => 2,
                    };

                    let left_condition =
                        self.build_where_helper(left, tables_with_aliases_map, Some(precedence));
                    let right_condition =
                        self.build_where_helper(right, tables_with_aliases_map, Some(precedence));
                    let operator = match op {
                        BinaryOperator::And => " && ",
                        BinaryOperator::Or => " || ",
                        _ => panic!("Invalid logical operator"),
                    };

                    let condition = format!("{}{}{}", left_condition, operator, right_condition);

                    if let Some(parent_precedence) = parent_precedence {
                        if precedence < parent_precedence {
                            return format!("({})", condition);
                        }
                    }
                    return condition;
                }
                _ => {
                    let left_condition = self.build_where_condition(left, tables_with_aliases_map);
                    let right_condition =
                        self.build_where_condition(right, tables_with_aliases_map);
                    let operator = match op {
                        BinaryOperator::Eq => " == ",
                        BinaryOperator::NotEq => " != ",
                        BinaryOperator::Gt => " > ",
                        BinaryOperator::GtEq => " >= ",
                        BinaryOperator::Lt => " < ",
                        BinaryOperator::LtEq => " <= ",
                        _ => panic!("Unknown comparison operator"),
                    };
                    format!("{}{}{}", left_condition, operator, right_condition)
                }
            },
            _ => panic!("Unsupported expression type"),
        }
    }

    fn build_where_condition(
        &self,
        expr: &Box<Expr>,
        tables_with_aliases_map: &HashMap<String, String>,
    ) -> String {
        match &**expr {
            Expr::CompoundIdentifier(ident) => {
                let alias = ident[0].to_string();
                let field = ident[1].to_string();

                // TODO: not the most efficient
                let table_name = tables_with_aliases_map
                    .iter()
                    .find(|(_, v)| *v == &alias)
                    .unwrap()
                    .0;

                let mapped_column_name = self
                    .schema_mapping
                    .get_column_name(table_name, &field)
                    .unwrap();
                format!("{}.{}.{}", self.row_selector, alias, mapped_column_name)
            }
            Expr::Identifier(ident) => ident.to_string(),
            Expr::Value(value) => value.to_string().replace("'", "\""),
            _ => panic!("Unsupported expression type"),
        }
    }

    fn build_joins(
        &self,
        table: &TableWithJoins,
        tables_with_aliases_map: &mut HashMap<String, String>,
        main_table_name: &str,
    ) -> String {
        let mut linq_query = String::new();

        linq_query.push_str(".Join(");
        for join in &table.joins {
            let table_name: String;
            if let sqlparser::ast::TableFactor::Table {
                name,
                alias: Some(alias),
                ..
            } = &join.relation
            {
                table_name = name.to_string();
                let table_alias = alias.to_string();

                let mapped_table_name = self.schema_mapping.get_table_name(&table_name).unwrap();

                tables_with_aliases_map.insert(table_name.to_string(), table_alias.to_string());

                linq_query.push_str(&format!("context.{}, ", mapped_table_name));
            } else {
                panic!("Unknown table factor type");
            }

            if let sqlparser::ast::JoinOperator::Inner(constraint) = &join.join_operator {
                if let sqlparser::ast::JoinConstraint::On(expr) = constraint {
                    if let sqlparser::ast::Expr::BinaryOp { left, right, .. } = expr {
                        let left_table_alias: String;
                        let left_table_field: String;

                        let right_table_alias: String;
                        let right_table_field: String;

                        if let Expr::CompoundIdentifier(ident) = &**left {
                            left_table_alias = ident[0].to_string();
                            left_table_field = ident[1].to_string();

                            let mapped_column_name = self
                                .schema_mapping
                                .get_column_name(&main_table_name, &left_table_field)
                                .unwrap();

                            linq_query.push_str(&format!(
                                "{} => {}.{}, ",
                                left_table_alias, left_table_alias, mapped_column_name
                            ));
                        } else {
                            panic!("Not a Compound Identifier");
                        }

                        if let Expr::CompoundIdentifier(ident) = &**right {
                            right_table_alias = ident[0].to_string();
                            right_table_field = ident[1].to_string();

                            let mapped_column_name = self
                                .schema_mapping
                                .get_column_name(&table_name, &right_table_field)
                                .unwrap();

                            linq_query.push_str(&format!(
                                "{} => {}.{}, ",
                                right_table_alias, right_table_alias, mapped_column_name
                            ));
                        } else {
                            panic!("Not a Compound Identifier");
                        }

                        linq_query.push_str(&format!(
                            "({}, {}) => new {{ {}, {} }})",
                            left_table_alias,
                            right_table_alias,
                            left_table_alias,
                            right_table_alias
                        ));
                    } else {
                        panic!("Unknown expression type");
                    }
                } else {
                    panic!("Unknown join constraint type");
                }
            } else {
                panic!("Unknown join operator type");
            }
        }
        return linq_query;
    }

    fn build_keywords(&self, query: &Box<Query>, selector: String) -> HashMap<String, String> {
        let mut keywords: HashMap<String, String> = HashMap::new();

        if query.order_by.len() > 0 {
            keywords.insert("order_by".to_string(), self.build_order_by(query, selector));
        }

        if query.limit.is_some() {
            keywords.insert("limit".to_string(), self.build_limit(query));
        }

        return keywords;
    }

    fn build_limit(&self, query: &Box<Query>) -> String {
        let mut linq_query = ".Take(".to_string();

        if let Some(limit) = &query.limit {
            if let Expr::Value(value) = limit {
                linq_query.push_str(&format!("{}", value));
            } else {
                panic!("Unknown expression type");
            }
        }

        linq_query.push_str(")");

        return linq_query;
    }

    fn build_order_by(&self, query: &Box<Query>, selector: String) -> String {
        let mut linq_query = String::new();

        for order_by in &query.order_by {
            if order_by.asc.unwrap() {
                linq_query.push_str(".OrderBy(");
            } else {
                linq_query.push_str(".OrderByDescending(");
            }

            if let Expr::Function(func) = &order_by.expr {
                if func.name.to_string().to_lowercase() == "count" {
                    linq_query.push_str(&format!("{} => {}.Count()", selector, selector));
                } else {
                    panic!("Unknown function");
                }
            } else {
                panic!("Unknown expression type");
            }
        }

        linq_query.push_str(")");

        return linq_query;
    }

    fn build_select(&self, select: &Box<Select>) -> HashMap<String, String> {
        let mut linq_query: HashMap<String, String> = HashMap::new();

        let main_table_name: String;
        let main_table_alias: String;

        let mut tables_with_aliases_map: HashMap<String, String> = HashMap::new();

        // TODO: can there be multiple tables in select.from? in which case?
        let table = &select.from[0];

        if let sqlparser::ast::TableFactor::Table { name, alias, .. } = &table.relation {
            if let Some(alias) = alias {
                main_table_alias = alias.to_string();
            } else {
                main_table_alias = String::new();
            }

            main_table_name = name.to_string();

            tables_with_aliases_map
                .insert(main_table_name.to_string(), main_table_alias.to_string());

            let mapped_table_name = self
                .schema_mapping
                .get_table_name(&main_table_name)
                .unwrap();

            linq_query.insert(
                "context".to_string(),
                format!("context.{}", mapped_table_name),
            );
        } else {
            panic!("Unknown table relation type");
        }

        if &table.joins.len() > &0 {
            linq_query.insert(
                "joins".to_string(),
                self.build_joins(table, &mut tables_with_aliases_map, &main_table_name),
            );
        }

        if select.selection.is_some() {
            linq_query.insert(
                "where".to_string(),
                self.build_where(select, &tables_with_aliases_map, &main_table_name),
            );
        }

        let is_only_function = select.projection.len() == 1
            && if let Some(SelectItem::UnnamedExpr(Expr::Function(_))) = select.projection.get(0) {
                true
            } else {
                false
            };

        let has_group_by = if let GroupByExpr::Expressions(expressions) = &select.group_by {
            expressions.len() > 0
        } else {
            false
        };

        let (group_by_query, group_by_fields) = self.build_group_by(select, &main_table_name);

        linq_query.insert("group_by".to_string(), group_by_query);

        let selector = if has_group_by {
            &self.group_selector
        } else {
            &self.row_selector
        };

        linq_query.insert("selector".to_string(), selector.to_string());

        if select.projection.len() > 0 {
            let mut current_linq_query = String::new();

            if select.projection.len() == 1 {
                if let SelectItem::UnnamedExpr(expr) = &select.projection[0] {
                    if let Expr::Function(function) = expr {
                        if function.name.to_string() == "COUNT" {
                            current_linq_query.push_str(&format!(".Count()"));
                        } else {
                            panic!("Unknown function");
                        }
                    } else {
                        current_linq_query.push_str(&self.build_projection(
                            select,
                            &tables_with_aliases_map,
                            &main_table_name,
                            has_group_by,
                            group_by_fields,
                        ));
                    }
                }
            } else {
                current_linq_query.push_str(&self.build_projection(
                    select,
                    &tables_with_aliases_map,
                    &main_table_name,
                    has_group_by,
                    group_by_fields,
                ));
            }

            linq_query.insert("projection".to_string(), current_linq_query);
        }

        // TODO: can be Distinct or Distinct On
        if select.distinct.is_some() {
            linq_query.insert("distinct".to_string(), ".Distinct()".to_string());
        }

        if is_only_function {
            linq_query.insert("final_aggregation".to_string(), ".ToList()".to_string());
        } else {
            linq_query.insert("final_aggregation".to_string(), ".ToList()".to_string());
        }

        return linq_query;
    }

    pub fn build_query_helper(&self, query: &Box<Query>) -> String {
        if let SetExpr::SetOperation {
            op,
            set_quantifier,
            left,
            right,
        } = &*query.body
        {
            let left_select: HashMap<String, String>;
            let right_select: HashMap<String, String>;

            if let SetExpr::Select(select) = &**left {
                left_select = self.build_select(select);
            } else {
                panic!("Unknown set expression type");
            }

            if let SetExpr::Select(select) = &**right {
                right_select = self.build_select(select);
            } else {
                panic!("Unknown set expression type");
            }

            let left_query = self.build_result(&left_select, &HashMap::new(), false, true);
            let right_query = self.build_result(&right_select, &HashMap::new(), false, true);

            return format!("{}.Intersect({}).ToList();", left_query, right_query);
        }

        let select = if let SetExpr::Select(select) = &*query.body {
            select
        } else {
            panic!("Unknown set expression type");
        };

        let select_result = self.build_select(select);
        let selector = select_result.get("selector").unwrap().to_string();
        let keywords_result = self.build_keywords(query, selector);

        return self.build_result(&select_result, &keywords_result, true, false);
    }

    fn build_result(
        &self,
        select_result: &HashMap<String, String>,
        keywords_result: &HashMap<String, String>,
        with_semicolon: bool,
        skip_final_aggregation: bool,
    ) -> String {
        let mut linq_query = select_result.get("context").unwrap().to_string();

        if let Some(from) = select_result.get("joins") {
            linq_query.push_str(from);
        }

        if let Some(where_clause) = select_result.get("where") {
            linq_query.push_str(where_clause);
        }

        if let Some(group_by) = select_result.get("group_by") {
            linq_query.push_str(group_by);
        }

        if let Some(order_by) = keywords_result.get("order_by") {
            linq_query.push_str(order_by);
        }

        if let Some(projection) = select_result.get("projection") {
            linq_query.push_str(projection);
        }

        if let Some(distinct) = select_result.get("distinct") {
            linq_query.push_str(distinct);
        }

        if let Some(limit) = keywords_result.get("limit") {
            linq_query.push_str(limit);
        }

        if !skip_final_aggregation {
            if let Some(final_aggregation) = select_result.get("final_aggregation") {
                linq_query.push_str(final_aggregation);
            }
        }

        if with_semicolon {
            linq_query = format!("{};", linq_query);
        }

        return linq_query;
    }

    pub fn build_query(&self, sql: &str) -> String {
        let dialect = GenericDialect {};
        let ast = Parser::parse_sql(&dialect, sql).unwrap();

        if let Statement::Query(query) = &ast[0] {
            return self.build_query_helper(query);
        } else {
            panic!("Unknown statement type");
        }
    }

    pub fn create_test(&self, sql: &str, linq_query: &str) -> String {
        let mut test = String::new();

        test.push_str(&format!(
            r##"
public static void Test() {{
    using var context = new Activity1Context();
    
    var query = "{}";
}}
        "##,
            sql
        ));

        return test;
    }
}

fn tests() {
    let mut queries_and_results: Vec<(&str, &str)> = Vec::new();

    queries_and_results.push((
        r#"SELECT rank FROM Faculty"#,
        r#"context.Faculties.Select(row => new { row.Rank }).ToList();"#,
    ));

    queries_and_results.push((
        r#"SELECT DISTINCT rank FROM Faculty"#,
        r#"context.Faculties.Select(row => new { row.Rank }).Distinct().ToList();"#,
    ));

    queries_and_results.push((
        r#"SELECT T1.fname, T1.lname FROM Faculty AS T1 JOIN Student AS T2 ON T1.FacID = T2.advisor WHERE T2.fname = "Linda" AND T2.lname = "Smith""#,
        r#"context.Faculties.Join(context.Students, T1 => T1.FacId, T2 => T2.Advisor, (T1, T2) => new { T1, T2 }).Where(row => row.T2.Fname == "Linda" && row.T2.Lname == "Smith").Select(row => new { row.T1.Fname, row.T1.Lname }).ToList();"#,
    ));

    queries_and_results.push((
        r#"SELECT COUNT(*), rank FROM Faculty GROUP BY rank"#,
        r#"context.Faculties.GroupBy(row => new { row.Rank }).Select(group => new { Count = group.Count(), group.Key.Rank }).ToList();"#,
    ));

    queries_and_results.push((
        r#"SELECT T1.FacID FROM Faculty AS T1 JOIN Student AS T2 ON T1.FacID  =  T2.advisor GROUP BY T1.FacID"#,
        r#"context.Faculties.Join(context.Students, T1 => T1.FacId, T2 => T2.Advisor, (T1, T2) => new { T1, T2 }).GroupBy(row => new { row.T1.FacId }).Select(group => new { group.Key.FacId }).ToList();"#,
    ));

    queries_and_results.push((
        r#"SELECT T1.activity_name FROM Activity AS T1 JOIN Faculty_participates_in AS T2 ON T1.actID  =  T2.actID GROUP BY T1.actID ORDER BY count(*) DESC LIMIT 1"#,
        "context.Activities.Join(context.FacultyParticipatesIns, T1 => T1.Actid, T2 => T2.Actid, (T1, T2) => new { T1, T2 }).GroupBy(row => new { row.T1.Actid }).OrderByDescending(group => group.Count()).Select(group => new { group.First().ActivityName }).Take(1).ToList();"
    ));

    queries_and_results.push((
        r#"SELECT T1.stuid FROM participates_in AS T1 JOIN activity AS T2 ON T1.actid  =  T2.actid WHERE T2.activity_name  =  'Canoeing' INTERSECT SELECT T1.stuid FROM participates_in AS T1 JOIN activity AS T2 ON T1.actid  =  T2.actid WHERE T2.activity_name  =  'Kayaking'"#,
        r#"context.ParticipatesIns.Join(context.Activities, T1 => T1.Actid, T2 => T2.Actid, (T1, T2) => new { T1, T2 }).Where(row => row.T2.ActivityName == "Canoeing").Select(row => new { row.T1.Stuid }).Intersect(context.ParticipatesIns.Join(context.Activities, T1 => T1.Actid, T2 => T2.Actid, (T1, T2) => new { T1, T2 }).Where(row => row.T2.ActivityName == "Kayaking").Select(row => new { row.T1.Stuid })).ToList();"#
    ));

    let linq_query_builder = LinqQueryBuilder::new("../entity-framework/Models/activity_1");

    for (index, (sql, expected_result)) in queries_and_results.iter().enumerate() {
        if index != 6 {
            continue;
        }

        let result = linq_query_builder.build_query(sql);

        if result == *expected_result {
            println!("Test {} passed", index + 1);
        } else {
            println!("Test {} failed", index + 1);
            println!("SQL: {}", sql);
            println!("Expected | Got");
            println!("{}\n{}", expected_result, result);
            return;
        }
    }
}

fn main() {
    tests();
}
