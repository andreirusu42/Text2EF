use std::collections::HashMap;

use sqlparser::ast::{BinaryOperator, Expr, SelectItem, SetExpr, Statement};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;

mod schema_mapping;

use schema_mapping::{create_schema_map, SchemaMapping};

pub struct LinqQueryBuilder {
    schema_mapping: SchemaMapping,
    selector: String,
}

impl LinqQueryBuilder {
    pub fn new(model_folder_path: &str) -> Self {
        let schema_mapping = create_schema_map(model_folder_path);

        LinqQueryBuilder {
            schema_mapping,
            selector: "row".to_string(),
        }
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

    fn build_condition(&self, expr: &Box<Expr>, table_name: &str) -> String {
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

    pub fn build_query(&self, sql: &str) -> String {
        let mut linq_query = "context.".to_string();

        let dialect = GenericDialect {};
        let ast = Parser::parse_sql(&dialect, sql).unwrap();

        if let Statement::Query(query) = &ast[0] {
            if let SetExpr::Select(select) = &*query.body {
                let mut main_table_name = String::new();
                let mut main_table_alias = String::new();

                let mut tables_with_aliases_map: HashMap<String, String> = HashMap::new();

                // TODO: can there be multiple tables in select.from? in which case?
                let table = &select.from[0];

                if let sqlparser::ast::TableFactor::Table {
                    name,
                    alias: Some(alias),
                    ..
                } = &table.relation
                {
                    main_table_name = name.to_string();
                    main_table_alias = alias.to_string();

                    tables_with_aliases_map
                        .insert(main_table_name.to_string(), main_table_alias.to_string());

                    let mapped_table_name = self
                        .schema_mapping
                        .get_table_name(&main_table_name)
                        .unwrap();

                    linq_query.push_str(&mapped_table_name);
                } else {
                    panic!("Unknown table factor type");
                }

                if &table.joins.len() > &0 {
                    linq_query.push_str(".Join(");
                }

                for join in &table.joins {
                    let mut table_name = String::new();
                    if let sqlparser::ast::TableFactor::Table {
                        name,
                        alias: Some(alias),
                        ..
                    } = &join.relation
                    {
                        table_name = name.to_string();
                        let table_alias = alias.to_string();

                        let mapped_table_name =
                            self.schema_mapping.get_table_name(&table_name).unwrap();

                        tables_with_aliases_map
                            .insert(table_name.to_string(), table_alias.to_string());

                        linq_query.push_str(&format!("context.{}, ", mapped_table_name));
                    } else {
                        panic!("Unknown table factor type");
                    }

                    if let sqlparser::ast::JoinOperator::Inner(constraint) = &join.join_operator {
                        if let sqlparser::ast::JoinConstraint::On(expr) = constraint {
                            if let sqlparser::ast::Expr::BinaryOp { left, op, right } = expr {
                                let mut left_table_alias = String::new();
                                let mut left_table_field = String::new();

                                let mut right_table_alias = String::new();
                                let mut right_table_field = String::new();

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
                        let left_condition = self.build_condition(left, &main_table_name);
                        linq_query = format!(
                            "{}{} => {}.{}",
                            linq_query, self.selector, self.selector, left_condition
                        );

                        if let BinaryOperator::And = *op {
                            linq_query.push_str(" && ");
                        } else {
                            panic!("Unknown operator");
                        }

                        let right_condition = self.build_condition(right, &main_table_name);
                        linq_query =
                            format!("{}{}.{})", linq_query, self.selector, right_condition);
                    } else {
                        panic!("Unknown expression type");
                    }
                }

                if select.projection.len() > 0 {
                    linq_query.push_str(&format!(".Select({} => new {{ ", self.selector));
                }
                let mut select_fields: Vec<String> = Vec::new();
                for select_item in &select.projection {
                    if let SelectItem::UnnamedExpr(expr) = select_item {
                        if let Expr::Identifier(identifier) = expr {
                            let column_name = identifier.to_string();
                            let table_alias = self.get_table_alias_from_field_name(
                                &tables_with_aliases_map,
                                &column_name,
                            );

                            let mapped_column_name = self
                                .schema_mapping
                                .get_column_name(&main_table_name, &column_name)
                                .unwrap();

                            select_fields.push(format!(
                                "{}.{}.{}",
                                self.selector, table_alias, mapped_column_name
                            ));
                        } else if let Expr::CompoundIdentifier(identifier) = expr {
                            let table_alias = identifier[0].to_string();
                            let column_name = identifier[1].to_string();

                            let mapped_column_name = self
                                .schema_mapping
                                .get_column_name(&main_table_name, &column_name)
                                .unwrap();

                            select_fields.push(format!(
                                "{}.{}.{}",
                                self.selector, table_alias, mapped_column_name
                            ));
                        } else {
                            panic!("Unknown expression type");
                        }
                    } else {
                        panic!("Unknown select item type");
                    }
                }

                linq_query.push_str(&select_fields.join(", "));
                linq_query.push_str(" })");
            }
        }

        linq_query.push_str(".ToList();");

        return linq_query;
    }
}

fn main() {
    let sql = r#"SELECT T1.fname, T1.lname FROM Faculty AS T1 JOIN Student AS T2 ON T1.FacID = T2.advisor WHERE T2.fname = "Linda" AND T2.lname = "Smith""#;

    let linq_query_builder = LinqQueryBuilder::new("../entity-framework/Models/activity_1");

    let linq_query = linq_query_builder.build_query(sql);

    println!("{}", linq_query);
}
