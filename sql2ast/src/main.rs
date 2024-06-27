use std::collections::HashMap;

use sqlparser::ast::{BinaryOperator, Expr, GroupByExpr, Select, SelectItem, SetExpr, Statement};
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
                            select_fields.push(format!("{} = {}.Key", column_name, selector));
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

                    select_fields.push(format!(
                        "{}.{}.{}",
                        self.row_selector, table_alias, mapped_column_name
                    ));
                } else if let Expr::Function(function) = expr {
                    if function.name.to_string() == "COUNT" {
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

    fn build_where_condition(&self, expr: &Box<Expr>, table_name: &str) -> String {
        let mut condition = String::new();

        if let Expr::BinaryOp { left, op, right } = &**expr {
            if let Expr::CompoundIdentifier(ident) = &**left {
                let alias = ident[0].to_string();
                let field = ident[1].to_string();

                let mapped_column_name = self
                    .schema_mapping
                    .get_column_name(&table_name, &field)
                    .unwrap();

                condition.push_str(&format!("{}.{}", alias, mapped_column_name));
            } else {
                panic!("Not a Compound Identifier");
            }

            if let BinaryOperator::Eq = *op {
                condition.push_str(" == ");
            } else {
                panic!("Unknown operator");
            }

            if let Expr::Identifier(ident) = &**right {
                condition.push_str(&format!("{}", ident.to_string()));
            } else {
                panic!("Unknown expression type");
            }
        } else {
            panic!("Unknown expression type");
        }

        return condition;
    }

    pub fn build_query_helper(&self, select: &Box<Select>) -> String {
        let mut linq_query: String = "context.".to_string();

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

            linq_query.push_str(&mapped_table_name);
        } else {
            panic!("Unknown table relation type");
        }

        if &table.joins.len() > &0 {
            linq_query.push_str(".Join(");
        }

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

        if select.selection.is_some() {
            linq_query.push_str(".Where(");
        }
        if let Some(selection) = &select.selection {
            if let Expr::BinaryOp { left, op, right } = selection {
                let left_condition = self.build_where_condition(left, &main_table_name);
                linq_query = format!(
                    "{}{} => {}.{}",
                    linq_query, self.row_selector, self.row_selector, left_condition
                );

                if let BinaryOperator::And = *op {
                    linq_query.push_str(" && ");
                } else {
                    panic!("Unknown operator");
                }

                let right_condition = self.build_where_condition(right, &main_table_name);
                linq_query = format!("{}{}.{})", linq_query, self.row_selector, right_condition);
            } else {
                panic!("Unknown expression type");
            }
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

        if let GroupByExpr::Expressions(expressions) = &select.group_by {
            for expr in expressions {
                if let Expr::Identifier(ident) = expr {
                    let column_name = ident.to_string();

                    let mapped_column_name = self
                        .schema_mapping
                        .get_column_name(&main_table_name, &column_name)
                        .unwrap();

                    linq_query.push_str(&format!(
                        ".GroupBy({} => {}.{})",
                        self.row_selector, self.row_selector, mapped_column_name
                    ));
                } else {
                    panic!("Unknown expression type");
                }
            }
        }

        if select.projection.len() > 0 {
            if select.projection.len() == 1 {
                if let SelectItem::UnnamedExpr(expr) = &select.projection[0] {
                    if let Expr::Function(function) = expr {
                        if function.name.to_string() == "COUNT" {
                            linq_query.push_str(&format!(".Count()"));
                        } else {
                            panic!("Unknown function");
                        }
                    } else {
                        linq_query.push_str(&self.build_projection(
                            select,
                            &tables_with_aliases_map,
                            &main_table_name,
                            has_group_by,
                        ));
                    }
                }
            } else {
                linq_query.push_str(&self.build_projection(
                    select,
                    &tables_with_aliases_map,
                    &main_table_name,
                    has_group_by,
                ));
            }
        }

        // TODO: can be Distinct or Distinct On
        if select.distinct.is_some() {
            linq_query.push_str(".Distinct()");
        }

        if is_only_function {
            // dunno yet
        } else {
            linq_query.push_str(".ToList();");
        }

        return linq_query;
    }

    pub fn build_query(&self, sql: &str) -> String {
        let dialect = GenericDialect {};
        let ast = Parser::parse_sql(&dialect, sql).unwrap();

        if let Statement::Query(query) = &ast[0] {
            if let SetExpr::Select(select) = &*query.body {
                return self.build_query_helper(select);
            } else {
                panic!("Unknown set expression type");
            }
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

fn main() {
    let mut sqls: Vec<&str> = Vec::new();
    // sqls.push(r#"SELECT rank FROM Faculty"#);
    // sqls.push(r#"SELECT DISTINCT rank FROM Faculty"#);
    // sqls.push(r#"SELECT COUNT(*), rank FROM Faculty GROUP BY rank"#);
    // sqls.push(r#"SELECT T1.fname, T1.lname FROM Faculty AS T1 JOIN Student AS T2 ON T1.FacID = T2.advisor WHERE T2.fname = "Linda" AND T2.lname = "Smith""#);
    sqls.push(r#"SELECT T1.FacID FROM Faculty AS T1 JOIN Student AS T2 ON T1.FacID  =  T2.advisor GROUP BY T1.FacID"#);

    let linq_query_builder = LinqQueryBuilder::new("../entity-framework/Models/activity_1");

    for sql in sqls {
        let linq_query = linq_query_builder.build_query(sql);
        println!("{}", linq_query);

        // let test = linq_query_builder.create_test(sql, &linq_query);
        // println!("{}", test);
    }
}
