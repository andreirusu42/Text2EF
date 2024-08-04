use std::collections::{HashMap, HashSet};
use std::hash::Hash;

use sqlparser::ast::{
    BinaryOperator, Expr, Function, FunctionArg, FunctionArgExpr, FunctionArguments, GroupByExpr,
    JoinConstraint, JoinOperator, Query, Select, SelectItem, SetExpr, Statement, Table, TableAlias,
    TableFactor, TableWithJoins,
};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;

use crate::case_insensitive_hashset::CaseInsensitiveHashSet;
use crate::determine_join_order;
use crate::schema_mapping::{Column, ColumnType, FieldType};

use super::case_insensitive_hashmap::CaseInsensitiveHashMap;
use super::determine_join_order::determine_join_order;
use super::schema_mapping::{create_schema_map, SchemaMapping};

#[derive(Clone, Debug)]
pub struct JoinOnWithTable {
    pub constraints: Vec<JoinOn>,
    pub table_alias: String,
    pub table_name: String,
}

#[derive(Clone, Debug, PartialEq)]
pub struct JoinOn {
    pub left_table_alias: String,
    pub left_table_field: String,
    pub right_table_alias: String,
    pub right_table_field: String,
    pub operator: String,
}

#[derive(Debug)]
pub struct ProjectionResult {
    pub select_result: String,
    pub group_by_result: String,
    pub calculated_fields: HashMap<String, String>,
    pub aggregated_fields: HashMap<String, String>,
}

#[derive(Debug)]
pub struct ProjectionSingleFunctionResult {
    pub result: String,
    pub field_type: String,
}

#[derive(Debug)]
pub struct SelectResult {
    pub linq_query: HashMap<String, String>,
    pub alias_to_table_map: CaseInsensitiveHashMap<TableAliasAndName>,
    pub calculated_fields: HashMap<String, String>,
    pub group_by_fields: Vec<String>,
    pub aggregated_fields: HashMap<String, String>,
}

#[derive(Debug)]
pub struct TableAliasAndName {
    pub mapped_alias: String,
    pub name: String,
}

pub struct LinqQueryBuilder {
    schema_mapping: SchemaMapping,
    row_selector: String,
    group_selector: String,
}

fn append_if_some(linq_query: &mut String, map: &HashMap<String, String>, key: &str) {
    if let Some(value) = map.get(key) {
        linq_query.push_str(value);
    }
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

    pub fn get_context_name(&self) -> &String {
        &self.schema_mapping.context
    }

    fn build_projection_single_function(
        &self,
        function: &Function,
        alias_to_table_map: &CaseInsensitiveHashMap<TableAliasAndName>,
    ) -> ProjectionSingleFunctionResult {
        let function_name = function.name.to_string().to_lowercase();
        let mut result = String::new();
        let field_type: String;

        let mapped_function_name = match function_name.as_str() {
            "count" => "Count",
            "sum" => "Sum",
            "avg" => "Average",
            "min" => "Min",
            "max" => "Max",
            _ => panic!("Unknown function"),
        };

        if ["count", "avg", "sum", "min", "max"].contains(&function_name.as_str()) {
            if let FunctionArguments::List(list) = &function.args {
                let is_distinct = if let Some(x) = list.duplicate_treatment {
                    x.to_string().to_lowercase() == "distinct"
                } else {
                    false
                };

                if list.args.len() == 1 {
                    if let FunctionArg::Unnamed(ident) = &list.args[0] {
                        if let FunctionArgExpr::Wildcard = ident {
                            field_type = "int".to_string();
                            // using .Distinct().[Func]() from below
                        } else if let FunctionArgExpr::Expr(expr) = ident {
                            let column_name: String;
                            let table_alias: String;

                            if let Expr::Identifier(ident) = expr {
                                column_name = ident.to_string();
                                table_alias = self
                                    .get_table_alias_from_field_name(
                                        &alias_to_table_map,
                                        &column_name,
                                    )
                                    .unwrap()
                                    .to_string();
                            } else if let Expr::CompoundIdentifier(ident) = expr {
                                table_alias = ident[0].to_string();
                                column_name = ident[1].to_string();
                            }
                            // else if let Expr::BinaryOp { left, op, right } = expr {
                            //     let left_expr = self.build_where_expr(
                            //         left,
                            //         alias_to_table_map,
                            //         false,
                            //         expr,
                            //         &HashMap::new(),
                            //     );
                            //     let right_expr = self.build_where_expr(
                            //         right,
                            //         alias_to_table_map,
                            //         false,
                            //         expr,
                            //         &HashMap::new(),
                            //     );

                            //     let operator = match op {
                            //         BinaryOperator::Minus => " - ",
                            //         _ => panic!("Unknown comparison operator"),
                            //     };

                            //     result.push_str(&format!(
                            //         ".{}({} => {}{}{})",
                            //         mapped_function_name,
                            //         self.row_selector,
                            //         left_expr,
                            //         operator,
                            //         right_expr
                            //     ));
                            //     return ProjectionSingleFunctionResult {
                            //         result,
                            //         field_type: "int".to_string(),
                            //     };
                            // }
                            else {
                                panic!("Invalid function argument");
                            }

                            let table = alias_to_table_map.get(&table_alias).unwrap();

                            let mapped_column = self
                                .schema_mapping
                                .get_column(&table.name, &column_name)
                                .unwrap();

                            field_type = match mapped_column.column_type {
                                ColumnType::Varchar => "string".to_string(),
                                ColumnType::Int => "int".to_string(),
                                ColumnType::Decimal => "double".to_string(),
                                ColumnType::Datetime => "DateTime".to_string(),
                                ColumnType::Bool => "bool".to_string(),
                                ColumnType::Double => "double".to_string(),
                                ColumnType::None => "".to_string(),
                            };

                            let stringify = if mapped_column.field_type == FieldType::Decimal
                                && mapped_column.column_type == ColumnType::Varchar
                            {
                                ".ToString()"
                            } else {
                                ""
                            };

                            let cast = if stringify.is_empty()
                                && (mapped_column.field_type == FieldType::Decimal
                                    || (function_name == "avg"
                                        && mapped_column.field_type != FieldType::Int
                                        && mapped_column.field_type != FieldType::String))
                            {
                                "(double) "
                            } else {
                                ""
                            };

                            if table_alias.is_empty() {
                                result.push_str(&format!(
                                    ".Select({} => {}{}.{}{})",
                                    self.row_selector,
                                    cast,
                                    self.row_selector,
                                    &mapped_column.name,
                                    stringify
                                ));
                            } else {
                                result.push_str(&format!(
                                    ".Select({} => {}{}.{}.{}{})",
                                    self.row_selector,
                                    cast,
                                    self.row_selector,
                                    table_alias,
                                    &mapped_column.name,
                                    stringify
                                ));
                            }

                            // This is the only way to do this afaik
                            if cast.is_empty()
                                && mapped_column.field_type == FieldType::String
                                && function_name == "avg"
                            {
                                result.push_str(
                                    ".ToList().Select(value => double.Parse(value)).ToList()",
                                );
                            }
                        } else {
                            panic!("Invalid function argument");
                        }

                        if is_distinct {
                            result.push_str(".Distinct()");
                        }

                        result.push_str(&format!(".{}()", mapped_function_name));
                    } else {
                        panic!("Invalid function argument");
                    }
                } else {
                    panic!("Invalid number of arguments for Count function");
                }
            } else {
                panic!("Invalid function arguments");
            }
        } else {
            panic!("Unknown function");
        }

        return ProjectionSingleFunctionResult { result, field_type };
    }

    fn build_projection(
        &self,
        select: &Box<Select>,
        alias_to_table_map: &CaseInsensitiveHashMap<TableAliasAndName>,
        has_group_by: bool,
        group_by_fields: &Vec<String>,
        use_new_object_for_select_when_single_field: bool,
        should_stringify_single_select: bool,
    ) -> ProjectionResult {
        let mut select_result = String::new();
        let mut group_by_result = String::new();
        let mut calculated_fields: HashMap<String, String> = HashMap::new();
        let mut aggregated_fields: HashMap<String, String> = HashMap::new();

        let mut selector = if has_group_by {
            &self.group_selector
        } else {
            &self.row_selector
        };

        let has_only_one_field = select.projection.len() == 1;
        let should_use_new = if has_only_one_field {
            use_new_object_for_select_when_single_field
        } else {
            true
        };

        let mut select_fields: Vec<String> = Vec::new();

        if !has_group_by {
            let mut number_of_functions = 0;

            for select_item in &select.projection {
                if let SelectItem::UnnamedExpr(expr) = select_item {
                    if let Expr::Function(..) = expr {
                        number_of_functions += 1;
                    }
                }
            }

            if number_of_functions > 1 {
                group_by_result.push_str(&format!(".GroupBy({} => 1)", selector));
                selector = &self.group_selector;
            }
        }

        select_result.push_str(&format!(".Select({} => ", selector));

        if should_use_new {
            select_result.push_str("new { ");
        }

        let mut fields_with_same_name: HashSet<String> = HashSet::new();
        let mut seen_fields: HashSet<String> = HashSet::new();

        for select_item in &select.projection {
            let expr = if let SelectItem::UnnamedExpr(expr) = select_item {
                expr
            } else {
                panic!("Unknown select item type");
            };

            let mapped_column_name: &String;

            if let Expr::Identifier(ident) = expr {
                let column_name = ident.to_string();

                let table_alias = self
                    .get_table_alias_from_field_name(&alias_to_table_map, &column_name)
                    .unwrap();

                let table = alias_to_table_map.get(table_alias).unwrap();

                mapped_column_name = self
                    .schema_mapping
                    .get_column_name(&table.name, &column_name)
                    .unwrap();
            } else if let Expr::CompoundIdentifier(identifier) = expr {
                let table_alias = identifier[0].to_string();
                let column_name = identifier[1].to_string();

                let table = alias_to_table_map.get(&table_alias).unwrap();

                mapped_column_name = self
                    .schema_mapping
                    .get_column_name(&table.name, &column_name)
                    .unwrap();
            } else if let Expr::Function(function) = expr {
                let arg_list = if let FunctionArguments::List(list) = &function.args {
                    &list.args
                } else {
                    panic!("Invalid function arguments");
                };

                let arg = if let FunctionArg::Unnamed(ident) = &arg_list[0] {
                    ident
                } else {
                    panic!("Invalid function argument");
                };

                let table_alias: String;

                let column_name = if let FunctionArgExpr::Expr(expr) = arg {
                    match expr {
                        Expr::Identifier(ident) => {
                            let column_name = ident.to_string();

                            table_alias = self
                                .get_table_alias_from_field_name(&alias_to_table_map, &column_name)
                                .unwrap()
                                .to_string();

                            column_name
                        }
                        Expr::CompoundIdentifier(ident) => {
                            table_alias = ident[0].to_string();
                            ident[1].to_string()
                        }
                        _ => panic!("Invalid function argument"),
                    }
                } else if let FunctionArgExpr::Wildcard = arg {
                    continue;
                } else {
                    panic!("Invalid function argument");
                };

                let table = alias_to_table_map.get(&table_alias).unwrap();

                mapped_column_name = self
                    .schema_mapping
                    .get_column_name(&table.name, &column_name)
                    .unwrap();
            } else {
                continue;
            }

            if seen_fields.contains(mapped_column_name) {
                fields_with_same_name.insert(mapped_column_name.clone());
            } else {
                seen_fields.insert(mapped_column_name.clone());
            }
        }

        let mut last_aggregate_function: Option<&Function> = None;
        for select_item in &select.projection {
            if let SelectItem::UnnamedExpr(expr) = select_item {
                if let Expr::Function(function) = expr {
                    let function_name = function.name.to_string().to_lowercase();

                    if ["min", "max"].contains(&function_name.as_str()) {
                        last_aggregate_function = Some(function);
                    }
                }
            }
        }

        let mut expressions: Vec<&Expr> = Vec::new();

        for select_item in &select.projection {
            let expr = if let SelectItem::UnnamedExpr(expr) = select_item {
                expr
            } else {
                panic!("Unknown select item type");
            };

            if let Expr::Nested(nested_expr) = expr {
                expressions.push(nested_expr);
            } else {
                expressions.push(expr);
            }
        }

        for expr in &expressions {
            if let Expr::Identifier(identifier) = expr {
                let column_name = identifier.to_string();

                let table_alias = self
                    .get_table_alias_from_field_name(&alias_to_table_map, &column_name)
                    .unwrap();

                let table = alias_to_table_map.get(table_alias).unwrap();

                let mapped_column = self
                    .schema_mapping
                    .get_column(&table.name, &column_name)
                    .unwrap();

                let stringify = if expressions.len() == 1
                    && should_stringify_single_select
                    && mapped_column.field_type != FieldType::String
                    && mapped_column.column_type == ColumnType::Varchar
                {
                    ".ToString()"
                } else {
                    ""
                };

                if table_alias.is_empty() {
                    if has_group_by {
                        if group_by_fields.contains(&mapped_column.name) {
                            select_fields.push(format!("{}.Key.{}", selector, mapped_column.name));
                        } else {
                            select_fields
                                .push(format!("{}.First().{}", selector, mapped_column.name));
                        }
                    } else {
                        select_fields
                            .push(format!("{}.{}{}", selector, mapped_column.name, stringify));
                    }
                } else {
                    if has_group_by {
                        if group_by_fields.contains(&mapped_column.name) {
                            select_fields.push(format!("{}.Key.{}", selector, mapped_column.name));
                        } else {
                            select_fields.push(format!(
                                "{}.First().{}.{}",
                                selector, table.mapped_alias, mapped_column.name
                            ));
                        }
                    } else {
                        select_fields.push(format!(
                            "{}.{}.{}",
                            selector, table.mapped_alias, mapped_column.name
                        ));
                    }
                }
            } else if let Expr::CompoundIdentifier(identifier) = expr {
                let table_alias = identifier[0].to_string();
                let column_name = identifier[1].to_string();

                let table = alias_to_table_map.get(&table_alias).unwrap();

                let mapped_column_name = self
                    .schema_mapping
                    .get_column_name(&table.name, &column_name)
                    .unwrap();

                let is_duplicated = fields_with_same_name.contains(mapped_column_name);

                /*
                   There's an amazing case. If you aggregate data by min/max, SQL will automatically sort the fields by that field too.
                   Hence you can't simply take .First() on the fields. You also need to sort them based on the last aggregate.

                   At the moment I simply do .OrderBy() on that field too.
                   I think it'd be a good idea to have a double select
                */

                if has_group_by {
                    if group_by_fields.contains(&mapped_column_name) {
                        select_fields.push(format!("{}.Key.{}", selector, mapped_column_name));
                    } else {
                        if let Some(last_aggregate_function) = last_aggregate_function {
                            let aggregation_type = last_aggregate_function.name.to_string();

                            let args = if let FunctionArguments::List(list) =
                                &last_aggregate_function.args
                            {
                                &list.args
                            } else {
                                panic!("Invalid function arguments");
                            };

                            let arg = if let FunctionArg::Unnamed(ident) = &args[0] {
                                ident
                            } else {
                                panic!("Invalid function argument");
                            };

                            let expr = if let FunctionArgExpr::Expr(expr) = arg {
                                expr
                            } else {
                                panic!("Invalid function argument");
                            };

                            let order_by_type = if aggregation_type == "min" {
                                "OrderBy"
                            } else if aggregation_type == "max" {
                                "OrderByDescending"
                            } else {
                                panic!("Invalid function argument");
                            };

                            let order_by_table_alias: String;
                            let order_by_mapped_column_name: &String;

                            if let Expr::Identifier(ident) = expr {
                                let column_name = ident.to_string();

                                order_by_table_alias = self
                                    .get_table_alias_from_field_name(
                                        &alias_to_table_map,
                                        &column_name,
                                    )
                                    .unwrap()
                                    .to_string();

                                let order_by_table =
                                    alias_to_table_map.get(&order_by_table_alias).unwrap();

                                order_by_mapped_column_name = self
                                    .schema_mapping
                                    .get_column_name(&order_by_table.name, &column_name)
                                    .unwrap();
                            } else if let Expr::CompoundIdentifier(ident) = expr {
                                order_by_table_alias = ident[0].to_string();
                                let column_name = ident[1].to_string();

                                let order_by_table =
                                    alias_to_table_map.get(&order_by_table_alias).unwrap();

                                order_by_mapped_column_name = self
                                    .schema_mapping
                                    .get_column_name(&order_by_table.name, &column_name)
                                    .unwrap();
                            } else {
                                panic!("Invalid function argument");
                            }

                            let order_by_table =
                                alias_to_table_map.get(&order_by_table_alias).unwrap();

                            select_fields.push(format!(
                                "{}.{}({} => {}.{}.{}).First().{}.{}",
                                selector,
                                order_by_type,
                                self.row_selector,
                                self.row_selector,
                                order_by_table.mapped_alias,
                                order_by_mapped_column_name,
                                table.mapped_alias,
                                mapped_column_name
                            ));
                        } else {
                            select_fields.push(format!(
                                "{}.First().{}.{}",
                                selector, table.mapped_alias, mapped_column_name
                            ));
                        }
                    }
                } else {
                    if is_duplicated {
                        let field_name = format!("{}{}", table.mapped_alias, mapped_column_name);

                        select_fields.push(format!(
                            "{} = {}.{}.{}",
                            field_name, selector, table.mapped_alias, mapped_column_name
                        ));
                    } else {
                        select_fields.push(format!(
                            "{}.{}.{}",
                            selector, table.mapped_alias, mapped_column_name
                        ));
                    }
                }
            } else if let Expr::Function(function) = expr {
                let function_name = function.name.to_string().to_lowercase();

                if !["sum", "min", "max", "avg", "count"].contains(&function_name.as_str()) {
                    panic!("Unknown function");
                }

                let column_name: String;
                let table_alias: String;

                let mapped_function_name = match function_name.as_str() {
                    "min" => "Min",
                    "max" => "Max",
                    "sum" => "Sum",
                    "count" => "Count",
                    "avg" => "Average",
                    _ => panic!("Unknown function"),
                };

                let arg_list = if let FunctionArguments::List(list) = &function.args {
                    list
                } else {
                    panic!("Invalid function arguments");
                };

                let is_distinct = if let Some(treatment) = arg_list.duplicate_treatment {
                    treatment.to_string().to_lowercase() == "distinct"
                } else {
                    false
                };

                if arg_list.args.len() != 1 {
                    panic!("Invalid number of arguments for function");
                }

                let ident = if let FunctionArg::Unnamed(ident) = &arg_list.args[0] {
                    ident
                } else {
                    panic!("Invalid function argument");
                };

                if let FunctionArgExpr::Wildcard = ident {
                    select_fields.push(format!(
                        "{} = {}.{}()",
                        mapped_function_name, selector, mapped_function_name
                    ));

                    aggregated_fields
                        .insert(function.to_string(), mapped_function_name.to_string());

                    continue;
                }

                let expr = if let FunctionArgExpr::Expr(expr) = ident {
                    expr
                } else {
                    panic!("Invalid function argument");
                };

                match expr {
                    Expr::Identifier(ident) => {
                        column_name = ident.to_string();
                        table_alias = self
                            .get_table_alias_from_field_name(&alias_to_table_map, &column_name)
                            .unwrap()
                            .to_string();
                    }
                    Expr::CompoundIdentifier(ident) => {
                        table_alias = ident[0].to_string();
                        column_name = ident[1].to_string();
                    }
                    _ => panic!("Invalid function argument"),
                }

                let table = alias_to_table_map.get(&table_alias).unwrap();

                let mapped_column = self
                    .schema_mapping
                    .get_column(&table.name, &column_name)
                    .unwrap();

                let distinct_name = if is_distinct { "Distinct" } else { "" };

                let suffix = if fields_with_same_name.contains(&mapped_column.name) {
                    table.mapped_alias.to_string()
                } else {
                    "".to_string()
                };

                let field_name = format!(
                    "{}{}{}{}",
                    mapped_function_name, distinct_name, &mapped_column.name, suffix
                );

                let cast = if mapped_column.field_type == FieldType::Decimal {
                    "(double) "
                } else {
                    ""
                };

                let select_expression = if table_alias.is_empty() {
                    format!("{}.{}", self.row_selector, &mapped_column.name)
                } else {
                    format!(
                        "{}.{}.{}",
                        self.row_selector, table.mapped_alias, &mapped_column.name
                    )
                };

                let function_call = if is_distinct {
                    format!(
                        "{}.Select({} => {}).Distinct().{}()",
                        selector, self.row_selector, select_expression, mapped_function_name
                    )
                } else {
                    format!(
                        "{}.Select({} => {}{}).{}()",
                        selector, self.row_selector, cast, select_expression, mapped_function_name
                    )
                };

                let result = format!("{} = {}", field_name, function_call);

                select_fields.push(result.to_string());

                // TODO: I guess that if you were to have an alias for this, then you should insert the alias. Just saying
                aggregated_fields.insert(function.to_string().to_lowercase(), field_name);
            } else if let Expr::BinaryOp { left, op, right } = expr {
                let left_expr = self.build_where_expr(
                    left,
                    alias_to_table_map,
                    false,
                    expr,
                    &aggregated_fields,
                );
                let right_expr = self.build_where_expr(
                    right,
                    alias_to_table_map,
                    false,
                    expr,
                    &aggregated_fields,
                );
                let operator = match op {
                    BinaryOperator::Minus => " - ",
                    _ => panic!("Unknown comparison operator"),
                };

                let field_name = match op {
                    BinaryOperator::Minus => "Diff",
                    _ => panic!("Unknown comparison operator"),
                };

                calculated_fields.insert(
                    format!("{}{}{}", left_expr, operator, right_expr),
                    field_name.to_string(),
                );
                select_fields.push(format!(
                    "{} = {}{}{}",
                    field_name, left_expr, operator, right_expr
                ));
            } else {
                panic!("Unknown expression type");
            }
        }

        select_result.push_str(&select_fields.join(", "));

        if should_use_new {
            select_result.push_str(" })");
        } else {
            select_result.push_str(")");
        }

        return ProjectionResult {
            select_result,
            group_by_result,
            calculated_fields,
            aggregated_fields,
        };
    }

    /*
       In Entity Framework, the behavior is stricter and more akin to standard SQL rules which require you to include all non-aggregated columns in the GROUP BY clause.
    */
    fn build_group_by(
        &self,
        select: &Box<Select>,
        alias_to_table_map: &CaseInsensitiveHashMap<TableAliasAndName>,
    ) -> (String, Vec<String>) {
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

                    let table_alias =
                        self.get_table_alias_from_field_name(alias_to_table_map, &column_name);

                    if table_alias.is_none() {
                        panic!("Unknown field name");
                    }

                    let table_alias = table_alias.unwrap();

                    let table = alias_to_table_map.get(table_alias).unwrap();

                    let mapped_column_name = self
                        .schema_mapping
                        .get_column_name(&table.name, &column_name)
                        .unwrap();

                    if table_alias.is_empty() {
                        group_by_fields
                            .push(format!("{}.{}", self.row_selector, mapped_column_name));
                    } else {
                        group_by_fields.push(format!(
                            "{}.{}.{}",
                            self.row_selector, table.mapped_alias, mapped_column_name
                        ));
                    }

                    raw_group_by_fields.push(mapped_column_name.to_string());
                } else if let Expr::CompoundIdentifier(identifiers) = expr {
                    let table_alias = identifiers[0].to_string();
                    let column_name = identifiers[1].to_string();

                    let table = alias_to_table_map.get(&table_alias).unwrap();

                    let mapped_column_name = self
                        .schema_mapping
                        .get_column_name(&table.name, &column_name)
                        .unwrap();

                    group_by_fields.push(format!(
                        "{}.{}.{}",
                        self.row_selector, table.mapped_alias, mapped_column_name
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
        alias_to_table_map: &'a CaseInsensitiveHashMap<TableAliasAndName>,
        field_name: &str,
    ) -> Option<&String> {
        let mut result: Option<&String> = None;

        for (table_alias, table) in alias_to_table_map {
            let columns = self.schema_mapping.get_table_columns(&table.name).unwrap();

            if columns.contains_key(&field_name.to_lowercase()) {
                if result.is_some() {
                    panic!("Ambiguous field name");
                }

                result = Some(table_alias);
            }
        }

        return result;
    }

    fn build_where(
        &self,
        select: &Box<Select>,
        alias_to_table_map: &CaseInsensitiveHashMap<TableAliasAndName>,
        aggregated_fields: &HashMap<String, String>,
    ) -> (String, String) {
        let mut selection_query = String::new();
        let mut having_query = String::new();

        if let Some(selection) = &select.selection {
            let where_clause = self.build_where_helper(
                selection,
                alias_to_table_map,
                None,
                false,
                &aggregated_fields,
            );
            selection_query = format!(".Where({} => {})", self.row_selector, where_clause);
        }

        if let Some(having) = &select.having {
            let having_clause =
                self.build_where_helper(having, alias_to_table_map, None, true, &aggregated_fields);
            having_query = format!(".Where({} => {})", self.group_selector, having_clause);
        }

        return (selection_query, having_query);
    }

    fn build_where_helper(
        &self,
        expr: &Expr,
        alias_to_table_map: &CaseInsensitiveHashMap<TableAliasAndName>,
        parent_precedence: Option<i32>,
        is_having: bool,
        aggregated_fields: &HashMap<String, String>,
    ) -> String {
        match expr {
            Expr::BinaryOp { left, op, right } => match op {
                BinaryOperator::And | BinaryOperator::Or => {
                    let precedence = match op {
                        BinaryOperator::And => 1,
                        BinaryOperator::Or => 0,
                        _ => 2,
                    };

                    let left_condition = self.build_where_helper(
                        left,
                        alias_to_table_map,
                        Some(precedence),
                        is_having,
                        aggregated_fields,
                    );
                    let right_condition = self.build_where_helper(
                        right,
                        alias_to_table_map,
                        Some(precedence),
                        is_having,
                        aggregated_fields,
                    );
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
                    let left_condition = self.build_where_expr(
                        left,
                        alias_to_table_map,
                        is_having,
                        expr,
                        &aggregated_fields,
                    );
                    let right_condition = self.build_where_expr(
                        right,
                        alias_to_table_map,
                        is_having,
                        expr,
                        &aggregated_fields,
                    );
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
            Expr::Like { expr, pattern, .. } => {
                let (alias, field) = match &**expr {
                    Expr::Identifier(ident) => {
                        let field = ident.to_string();
                        let alias = self
                            .get_table_alias_from_field_name(alias_to_table_map, &field)
                            .unwrap();
                        (alias.to_string(), field)
                    }
                    Expr::CompoundIdentifier(ident) => {
                        let alias = ident[0].to_string();
                        let field = ident[1].to_string();
                        (alias, field)
                    }
                    _ => panic!("Unexpected expression type"),
                };

                let table = alias_to_table_map.get(&alias).unwrap();
                let mapped_column = self.schema_mapping.get_column(&table.name, &field).unwrap();
                let value = pattern.to_string().replace("'", "\"");

                let alias_string = if table.mapped_alias.is_empty() {
                    "".to_string()
                } else {
                    format!("{}.", table.mapped_alias)
                };

                let stringify = if mapped_column.field_type != FieldType::String
                    && mapped_column.column_type == ColumnType::Varchar
                {
                    ".ToString()"
                } else {
                    ""
                };

                return format!(
                    "EF.Functions.Like({}.{}{}{}, {})",
                    self.row_selector, alias_string, mapped_column.name, stringify, value
                );
            }
            Expr::InSubquery {
                subquery,
                expr,
                negated,
            } => {
                let built_subquery = self.build_query_helper(
                    subquery,
                    Some(false),
                    Some(false),
                    Some(true),
                    Some(false),
                );

                let column_name: String;
                let table_alias: String;

                match &**expr {
                    Expr::Identifier(ident) => {
                        column_name = ident.to_string();
                        table_alias = self
                            .get_table_alias_from_field_name(alias_to_table_map, &column_name)
                            .unwrap()
                            .to_string();
                    }
                    Expr::CompoundIdentifier(ident) => {
                        table_alias = ident[0].to_string();
                        column_name = ident[1].to_string();
                    }
                    _ => panic!("Unsupported expression type"),
                }

                let table = alias_to_table_map.get(&table_alias).unwrap();
                let mapped_column_name = self
                    .schema_mapping
                    .get_column_name(&table.name, &column_name)
                    .unwrap();

                let operator = if *negated { "!" } else { "" };

                let table_alias_string = if table_alias.is_empty() {
                    "".to_string()
                } else {
                    format!("{}.", table_alias)
                };

                return format!(
                    "{}{}.Contains({}.{}{})",
                    operator,
                    built_subquery,
                    self.row_selector,
                    table_alias_string,
                    mapped_column_name,
                );
            }
            Expr::Between {
                low,
                high,
                expr,
                negated,
            } => {
                let low_condition = self.build_where_expr(
                    low,
                    alias_to_table_map,
                    is_having,
                    expr,
                    &HashMap::new(),
                );
                let high_condition = self.build_where_expr(
                    high,
                    alias_to_table_map,
                    is_having,
                    expr,
                    &HashMap::new(),
                );
                let expr_condition = self.build_where_expr(
                    expr,
                    alias_to_table_map,
                    is_having,
                    expr,
                    &HashMap::new(),
                );

                if *negated {
                    return format!(
                        "{} < {} || {} > {}",
                        expr_condition, high_condition, expr_condition, low_condition
                    );
                }

                return format!(
                    "{} >= {} && {} <= {}",
                    expr_condition, low_condition, expr_condition, high_condition
                );
            }
            _ => panic!("Unsupported expression type"),
        }
    }

    fn build_where_expr(
        &self,
        expr: &Box<Expr>,
        alias_to_table_map: &CaseInsensitiveHashMap<TableAliasAndName>,
        is_having: bool,
        root_expr: &Expr,
        aggregated_fields: &HashMap<String, String>,
    ) -> String {
        let selector = if is_having {
            &self.group_selector
        } else {
            &self.row_selector
        };

        match &**expr {
            Expr::CompoundIdentifier(ident) => {
                let alias = ident[0].to_string();
                let field = ident[1].to_string();

                let table = alias_to_table_map.get(&alias).unwrap();

                let mapped_column_name = self
                    .schema_mapping
                    .get_column_name(&table.name, &field)
                    .unwrap();
                format!("{}.{}.{}", selector, table.mapped_alias, mapped_column_name)
            }
            Expr::Identifier(ident) => {
                let field = ident.to_string();

                let alias_option = self.get_table_alias_from_field_name(alias_to_table_map, &field);

                if alias_option.is_none() {
                    return field; // this happens when using double quotes on a string
                }

                let alias = alias_option.unwrap();

                let table = alias_to_table_map.get(alias).unwrap();

                let mapped_column = self.schema_mapping.get_column(&table.name, &field).unwrap();

                if alias.is_empty() {
                    return format!("{}.{}", selector, mapped_column.name);
                }

                format!("{}.{}.{}", selector, table.mapped_alias, mapped_column.name)
            }
            Expr::Function(func) => {
                let function_name = func.name.to_string().to_lowercase();

                let aggregated_field_option =
                    aggregated_fields.get(&func.to_string().to_lowercase());

                if let Some(aggregated_field) = aggregated_field_option {
                    return format!("{}.{}", selector, aggregated_field);
                }

                let args = if let FunctionArguments::List(list) = &func.args {
                    &list.args
                } else {
                    panic!("Invalid function arguments");
                };

                let mapped_function_name = match function_name.as_str() {
                    "count" => "Count",
                    "sum" => "Sum",
                    "avg" => "Average",
                    "min" => "Min",
                    "max" => "Max",
                    _ => panic!("Unknown function"),
                };

                if args.len() != 1 {
                    panic!("Invalid number of arguments for function");
                }

                let function_arg_expr = if let FunctionArg::Unnamed(ident) = &args[0] {
                    ident
                } else {
                    panic!("Invalid function argument");
                };

                if let FunctionArgExpr::Wildcard = function_arg_expr {
                    return format!("{}.{}()", selector, mapped_function_name);
                }

                let expr = if let FunctionArgExpr::Expr(expr) = &function_arg_expr {
                    expr
                } else {
                    panic!("Invalid function argument");
                };

                match expr {
                    Expr::Identifier(ident) => {
                        let field = ident.to_string();

                        let alias_option =
                            self.get_table_alias_from_field_name(alias_to_table_map, &field);

                        let alias = alias_option.unwrap();

                        let table = alias_to_table_map.get(alias).unwrap();

                        let mapped_column_name = self
                            .schema_mapping
                            .get_column_name(&table.name, &field)
                            .unwrap();

                        let suffix = if function_name == "count" {
                            " != null"
                        } else {
                            ""
                        };
                        let formatted_string = if alias.is_empty() {
                            format!(
                                "{}.{}({} => {}.{}{})",
                                selector,
                                mapped_function_name,
                                self.row_selector,
                                self.row_selector,
                                mapped_column_name,
                                suffix
                            )
                        } else {
                            format!(
                                "{}.{}({} => {}.{}.{}{})",
                                selector,
                                mapped_function_name,
                                self.row_selector,
                                self.row_selector,
                                alias,
                                mapped_column_name,
                                suffix
                            )
                        };

                        formatted_string
                    }
                    Expr::CompoundIdentifier(ident) => {
                        let alias = ident[0].to_string();
                        let field = ident[1].to_string();

                        let table = alias_to_table_map.get(&alias).unwrap();

                        let mapped_column_name = self
                            .schema_mapping
                            .get_column_name(&table.name, &field)
                            .unwrap();

                        return format!(
                            "{}.{}({} => {}.{}.{})",
                            selector,
                            mapped_function_name,
                            self.row_selector,
                            self.row_selector,
                            alias,
                            mapped_column_name
                        );
                    }
                    _ => panic!("Unsupported expression type"),
                }
            }
            Expr::Value(value) => {
                let left = match &root_expr {
                    Expr::BinaryOp { left, .. } => left,
                    _ => return value.to_string(),
                };

                let (alias, field) = match &**left {
                    Expr::Identifier(ident) => {
                        let field = ident.to_string();
                        let alias = self
                            .get_table_alias_from_field_name(alias_to_table_map, &field)
                            .unwrap();
                        (alias.to_string(), field)
                    }
                    Expr::CompoundIdentifier(ident) => {
                        let alias = ident[0].to_string();
                        let field = ident[1].to_string();
                        (alias, field)
                    }
                    _ => return value.to_string().replace("'", "\""),
                };

                let table = alias_to_table_map.get(&alias).unwrap();

                let mapped_column = self.schema_mapping.get_column(&table.name, &field).unwrap();

                if mapped_column.field_type == FieldType::Bool {
                    let number = match value {
                        sqlparser::ast::Value::Number(num, ..) => num,
                        _ => panic!("Invalid value type"),
                    };

                    if number == "0" {
                        return "false".to_string();
                    } else {
                        return "true".to_string();
                    }
                }

                let value = value.to_string().replace("'", "\"");

                if mapped_column.field_type == FieldType::String && !value.starts_with("\"") {
                    return format!("\"{}\"", value);
                }

                return value;
            }
            Expr::Subquery(subquery) => {
                let subquery = self.build_query_helper(
                    subquery,
                    Some(false),
                    Some(false),
                    Some(true),
                    Some(false),
                );

                // TODO: This is a bit hacky

                // if the subquery ends with .Min(), .Max(), .Count(), then return the subquery

                let aggregation_functions = ["Min", "Max", "Count", "Average"];

                for function in aggregation_functions.iter() {
                    if subquery.ends_with(&format!(".{}()", function)) {
                        return subquery;
                    }
                }

                if let Expr::BinaryOp { left, .. } = &root_expr {
                    if let Expr::Identifier(_) = &**left {
                        return format!("{}.First()", subquery);
                    }

                    if let Expr::CompoundIdentifier(_) = &**left {
                        return format!("{}.First()", subquery);
                    }
                }

                return subquery;
            }

            _ => panic!("Unsupported expression type"),
        }
    }

    fn build_join_on(&self, expr: &Expr, joins: &mut Vec<JoinOn>) -> () {
        let (left, op, right) = match expr {
            Expr::BinaryOp { left, op, right } => (left, op, right),
            _ => panic!("Invalid expression type"),
        };

        if let (Expr::CompoundIdentifier(left_ident), Expr::CompoundIdentifier(right_ident)) =
            (&**left, &**right)
        {
            let left_table_alias = left_ident[0].to_string();
            let left_table_field = left_ident[1].to_string();

            let right_table_alias = right_ident[0].to_string();
            let right_table_field = right_ident[1].to_string();

            let join_condition = JoinOn {
                left_table_alias: left_table_alias.to_lowercase(),
                left_table_field: left_table_field.clone(),
                right_table_alias: right_table_alias.to_lowercase(),
                right_table_field: right_table_field.clone(),
                operator: op.to_string(),
            };

            joins.push(join_condition);

            return;
        }

        if let Expr::BinaryOp { .. } = &**left {
            self.build_join_on(left, joins);
        }

        if let Expr::BinaryOp { .. } = &**right {
            self.build_join_on(right, joins);
        }
    }

    fn rearrange_joins(&self, joins: &mut Vec<JoinOnWithTable>, main_table_alias: &str) {
        let number_of_joins = joins.len();

        if number_of_joins < 2 {
            return;
        }

        let mut all_join_constraints: Vec<JoinOn> = Vec::new();
        let mut all_constraints: Vec<Vec<(&str, &str)>> = Vec::new();

        for join in joins.iter() {
            all_join_constraints.append(&mut join.constraints.clone());
            all_constraints.push(
                join.constraints
                    .iter()
                    .map(|c| (c.left_table_alias.as_str(), c.right_table_alias.as_str()))
                    .collect(),
            );
        }

        let all_tables = joins.iter().map(|join| join.table_alias.as_str()).collect();

        let mut aliases_in_correct_order_to_join = determine_join_order(
            &mut all_constraints,
            all_tables,
            &main_table_alias.to_lowercase(),
        );
        let mut aliases_known_so_far: CaseInsensitiveHashSet<String> =
            CaseInsensitiveHashSet::new();
        aliases_known_so_far.insert(
            aliases_in_correct_order_to_join
                .first()
                .unwrap()
                .to_string(),
        );
        aliases_in_correct_order_to_join.remove(0);

        let mut new_joins: Vec<JoinOnWithTable> = Vec::new();

        for alias in aliases_in_correct_order_to_join {
            aliases_known_so_far.insert(alias.to_string());
            let mut new_constraints: Vec<JoinOn> = Vec::new();

            let join: JoinOnWithTable = joins
                .iter()
                .find(|join| join.table_alias == alias.to_lowercase())
                .unwrap()
                .clone();

            for constraint in &all_join_constraints {
                if aliases_known_so_far.contains(&constraint.left_table_alias)
                    && aliases_known_so_far.contains(&constraint.right_table_alias)
                {
                    new_constraints.push(constraint.clone());
                }
            }

            all_join_constraints.retain(|c| !new_constraints.contains(c));

            new_joins.push(JoinOnWithTable {
                table_name: join.table_name,
                table_alias: join.table_alias,
                constraints: new_constraints,
            });
        }

        joins.clear();
        joins.append(&mut new_joins);
    }

    fn build_joins(
        &self,
        table: &TableWithJoins,
        main_table_alias: &str,
        alias_to_table_map: &mut CaseInsensitiveHashMap<TableAliasAndName>,
    ) -> String {
        let mut linq_query = String::new();
        let mut joined_aliases: Vec<String> = Vec::new();
        let mut joins: Vec<JoinOnWithTable> = Vec::new();

        for join in &table.joins {
            let table_alias: String;
            let table_name: String;
            let mapped_table_alias: String;

            if let TableFactor::Table {
                name,
                alias: Some(alias),
                ..
            } = &join.relation
            {
                table_name = name.to_string();
                table_alias = alias.to_string();
                mapped_table_alias = alias.to_string();

                alias_to_table_map.insert(
                    table_alias.to_string(),
                    TableAliasAndName {
                        mapped_alias: mapped_table_alias.to_string(),
                        name: table_name.to_string(),
                    },
                );
            } else if let TableFactor::Table { name, .. } = &join.relation {
                table_name = name.to_string();
                table_alias = name.to_string();
                mapped_table_alias = self
                    .schema_mapping
                    .get_table_name(&table_name)
                    .unwrap()
                    .to_string();

                alias_to_table_map.insert(
                    table_alias.to_string(),
                    TableAliasAndName {
                        mapped_alias: mapped_table_alias.to_string(),
                        name: table_name.to_string(),
                    },
                );
            } else {
                panic!("Unknown table factor type");
            }

            let mut current_joins: Vec<JoinOn> = Vec::new();

            if let sqlparser::ast::JoinOperator::Inner(constraint) = &join.join_operator {
                if let sqlparser::ast::JoinConstraint::On(expr) = constraint {
                    self.build_join_on(&expr, &mut current_joins);
                }
            } else {
                panic!("Unknown join operator type");
            }

            joins.push(JoinOnWithTable {
                table_name,
                table_alias: table_alias.to_lowercase(),
                constraints: current_joins,
            });
        }

        self.rearrange_joins(&mut joins, main_table_alias);

        for join in joins {
            let mapped_table_name = self
                .schema_mapping
                .get_table_name(&join.table_name)
                .unwrap();

            let table = alias_to_table_map.get(&join.table_alias).unwrap();

            if join.constraints.len() == 1 {
                linq_query.push_str(&format!(".Join(context.{}, ", mapped_table_name));

                let constraint = &join.constraints[0];
                let mut left_table_alias = &constraint.left_table_alias;
                let mut right_table_alias = &constraint.right_table_alias;

                let mut left_table_field = &constraint.left_table_field;
                let mut right_table_field = &constraint.right_table_field;

                let right_table = alias_to_table_map.get(right_table_alias).unwrap();
                if joined_aliases.contains(&right_table.mapped_alias)
                    || main_table_alias.to_lowercase() == right_table_alias.to_lowercase()
                // this is the case when you join the table in reverse order, only ƒor the first constraint
                {
                    let temp_table_alias = left_table_alias;
                    let temp_table_field = left_table_field;

                    left_table_alias = right_table_alias;
                    left_table_field = right_table_field;

                    right_table_alias = temp_table_alias;
                    right_table_field = temp_table_field;
                }

                let left_table = alias_to_table_map.get(left_table_alias).unwrap();
                let right_table = alias_to_table_map.get(right_table_alias).unwrap();

                let mapped_left_field = self
                    .schema_mapping
                    .get_column_name(&left_table.name, &left_table_field)
                    .unwrap();

                let mapped_right_field = self
                    .schema_mapping
                    .get_column_name(&right_table.name, &right_table_field)
                    .unwrap();

                let outer_key_selector: String;

                if joined_aliases.len() == 0 {
                    outer_key_selector = left_table.mapped_alias.to_string();
                    joined_aliases.push(left_table.mapped_alias.clone());
                } else {
                    outer_key_selector = "joined".to_string()
                }

                if outer_key_selector == "joined" {
                    let mut joined_aliases_str = String::new();

                    for alias in &joined_aliases {
                        joined_aliases_str.push_str(&format!("joined.{}, ", alias));
                    }

                    linq_query.push_str(&format!(
                        "{} => {}.{}.{}, ",
                        outer_key_selector,
                        outer_key_selector,
                        left_table.mapped_alias,
                        mapped_left_field
                    ));

                    linq_query.push_str(&format!(
                        "{} => {}.{}, ",
                        right_table.mapped_alias, right_table.mapped_alias, mapped_right_field
                    ));

                    linq_query.push_str(&format!(
                        "({}, {}) => new {{ {}{} }})",
                        outer_key_selector,
                        right_table.mapped_alias,
                        joined_aliases_str,
                        right_table.mapped_alias
                    ));
                } else {
                    linq_query.push_str(&format!(
                        "{} => {}.{}, {} => {}.{}, ({}, {}) => new {{ {}, {} }})",
                        left_table.mapped_alias,
                        left_table.mapped_alias,
                        mapped_left_field,
                        right_table.mapped_alias,
                        right_table.mapped_alias,
                        mapped_right_field,
                        left_table.mapped_alias,
                        right_table.mapped_alias,
                        left_table.mapped_alias,
                        right_table.mapped_alias
                    ));
                }

                joined_aliases.push(right_table.mapped_alias.clone());
            } else if join.constraints.len() == 0 {
                if joined_aliases.len() > 0 {
                    let mut joined_aliases_str = String::new();
                    for alias in &joined_aliases {
                        joined_aliases_str.push_str(&format!("joined.{}, ", alias));
                    }

                    linq_query.push_str(&format!(
                        ".SelectMany(s => context.{}, (joined, {}) => new {{ {}{} }})",
                        mapped_table_name,
                        table.mapped_alias,
                        joined_aliases_str,
                        table.mapped_alias
                    ));
                } else {
                    linq_query.push_str(&format!(
                        ".SelectMany(s => context.{}, ({}, {}) => new {{ {}, {} }})",
                        mapped_table_name,
                        main_table_alias,
                        table.mapped_alias,
                        main_table_alias,
                        table.mapped_alias
                    ));
                }
            } else {
                linq_query.push_str(&format!(".Join(context.{}, ", mapped_table_name));

                let mut left_join_fields: Vec<String> = Vec::new();
                let mut right_join_fields: Vec<String> = Vec::new();

                let mut last_table_alias = String::new();

                for (index, constraint) in join.constraints.iter().enumerate() {
                    let mut left_table_alias = &constraint.left_table_alias;
                    let mut right_table_alias = &constraint.right_table_alias;

                    let mut left_table_field = &constraint.left_table_field;
                    let mut right_table_field = &constraint.right_table_field;

                    let mapped_right_table = alias_to_table_map.get(right_table_alias).unwrap();
                    if joined_aliases.contains(&mapped_right_table.mapped_alias)
                    // this is the case when you join the table in reverse order
                    {
                        let temp_table_alias = left_table_alias;
                        let temp_table_field = left_table_field;

                        left_table_alias = right_table_alias;
                        left_table_field = right_table_field;

                        right_table_alias = temp_table_alias;
                        right_table_field = temp_table_field;
                    }

                    let left_table = alias_to_table_map.get(left_table_alias).unwrap();
                    let right_table = alias_to_table_map.get(right_table_alias).unwrap();

                    let mapped_left_field = self
                        .schema_mapping
                        .get_column_name(&left_table.name, &left_table_field)
                        .unwrap();

                    let mapped_right_field = self
                        .schema_mapping
                        .get_column_name(&right_table.name, &right_table_field)
                        .unwrap();

                    let prefix = if joined_aliases.len() == 0 {
                        "".to_string()
                    } else {
                        "joined.".to_string()
                    };

                    left_join_fields.push(format!(
                        "Pair{} = {}{}.{}",
                        index + 1,
                        prefix,
                        left_table.mapped_alias,
                        mapped_left_field
                    ));
                    right_join_fields.push(format!(
                        "Pair{} = {}{}.{}",
                        index + 1,
                        prefix,
                        right_table.mapped_alias,
                        mapped_right_field
                    ));

                    last_table_alias = right_table.mapped_alias.clone();
                }

                let left_selector = if joined_aliases.len() == 0 {
                    main_table_alias.to_string()
                } else {
                    "joined".to_string()
                };

                let mut joined_aliases_str = String::new();
                for alias in &joined_aliases {
                    joined_aliases_str.push_str(&format!("joined.{}, ", alias));
                }

                let left_table_alias = if joined_aliases.len() == 0 {
                    format!("{}, ", main_table_alias)
                } else {
                    "".to_string()
                };

                linq_query.push_str(&format!(
                    "{} => new {{ {} }}, {} => new {{ {} }}, ({}, {}) => new {{ {}{}{} }})",
                    left_selector,
                    left_join_fields.join(", "),
                    last_table_alias,
                    right_join_fields.join(", "),
                    left_selector,
                    last_table_alias,
                    joined_aliases_str,
                    left_table_alias,
                    last_table_alias
                ));

                joined_aliases.push(last_table_alias);
            }
        }

        return linq_query;
    }

    fn build_keywords(
        &self,
        query: &Box<Query>,
        selector: String,
        alias_to_table_map: &CaseInsensitiveHashMap<TableAliasAndName>,
        group_by_fields: &Vec<String>,
        calculated_fields: &HashMap<String, String>,
        aggregated_fields: &HashMap<String, String>,
    ) -> HashMap<String, String> {
        let mut keywords: HashMap<String, String> = HashMap::new();

        if query.order_by.len() > 0 {
            keywords.insert(
                "order_by".to_string(),
                self.build_order_by(
                    query,
                    selector,
                    alias_to_table_map,
                    group_by_fields,
                    calculated_fields,
                    aggregated_fields,
                ),
            );
        }

        if query.limit.is_some() {
            keywords.insert("limit".to_string(), self.build_limit(query));
        }

        return keywords;
    }

    fn build_limit(&self, query: &Box<Query>) -> String {
        let mut linq_query = ".Take(".to_string();

        if let Some(limit) = &query.limit {
            match limit {
                Expr::Value(value) => {
                    linq_query.push_str(&format!("{}", value));
                }
                _ => panic!("Unknown expression type"),
            }
        }

        linq_query.push_str(")");

        return linq_query;
    }

    fn build_order_by(
        &self,
        query: &Box<Query>,
        selector: String,
        alias_to_table_map: &CaseInsensitiveHashMap<TableAliasAndName>,
        group_by_fields: &Vec<String>,
        calculated_fields: &HashMap<String, String>,
        aggregated_fields: &HashMap<String, String>,
    ) -> String {
        let mut linq_query = String::new();
        let mut has_ordered_one_so_far = false;

        for order_by in &query.order_by {
            let is_ordering_asc = if let Some(is_ordering_asc) = order_by.asc {
                is_ordering_asc
            } else {
                true
            };

            if is_ordering_asc {
                let val = if has_ordered_one_so_far {
                    ".ThenBy("
                } else {
                    ".OrderBy("
                };

                linq_query.push_str(val);
            } else {
                let val = if has_ordered_one_so_far {
                    ".ThenByDescending("
                } else {
                    ".OrderByDescending("
                };

                linq_query.push_str(val);
            }

            has_ordered_one_so_far = true;

            if let Expr::Function(func) = &order_by.expr {
                let function_name = func.name.to_string().to_lowercase();

                if !["count", "avg", "sum", "max"].contains(&function_name.as_str()) {
                    panic!("Unknown function");
                }

                let args = if let FunctionArguments::List(list) = &func.args {
                    &list.args
                } else {
                    panic!("Invalid function arguments");
                };

                if args.len() != 1 {
                    panic!("Invalid number of arguments for count/avg function");
                }

                let mapped_function_name = match function_name.as_str() {
                    "count" => "Count",
                    "sum" => "Sum",
                    "avg" => "Average",
                    "max" => "Max",
                    _ => panic!("Unknown function"),
                };

                let ident = if let FunctionArg::Unnamed(ident) = &args[0] {
                    ident
                } else {
                    panic!("Invalid function argument");
                };

                if let FunctionArgExpr::Wildcard = ident {
                    let aggregated_field_expr = func.to_string();

                    let aggregated_field = aggregated_fields.get(&aggregated_field_expr);

                    if let Some(aggregated_field) = aggregated_field {
                        linq_query.push_str(&format!(
                            "{} => {}.{}",
                            selector, selector, aggregated_field
                        ));
                    } else {
                        linq_query.push_str(&format!(
                            "{} => {}.{}()",
                            selector, selector, mapped_function_name
                        ));
                    }
                } else if let FunctionArgExpr::Expr(expr) = ident {
                    let column_name: String;
                    let table_alias: String;

                    if let Expr::Identifier(ident) = expr {
                        column_name = ident.to_string();
                        table_alias = self
                            .get_table_alias_from_field_name(&alias_to_table_map, &column_name)
                            .unwrap()
                            .to_string();
                    } else if let Expr::CompoundIdentifier(ident) = expr {
                        table_alias = ident[0].to_string();
                        column_name = ident[1].to_string();
                    } else {
                        panic!("Invalid function argument");
                    }

                    let table = alias_to_table_map.get(&table_alias).unwrap();

                    let mapped_column = self
                        .schema_mapping
                        .get_column(&table.name, &column_name)
                        .unwrap();

                    let cast = if mapped_column.field_type == FieldType::Decimal {
                        "(double) "
                    } else {
                        ""
                    };

                    let aggregated_field_expr = if table_alias.is_empty() {
                        format!("{}({})", function_name, column_name).to_lowercase()
                    } else {
                        format!("{}({}.{})", function_name, table_alias, column_name).to_lowercase()
                    };

                    let aggregated_field = aggregated_fields.get(&aggregated_field_expr);

                    if let Some(aggregated_field) = aggregated_field {
                        linq_query.push_str(&format!(
                            "{} => {}.{}",
                            selector, selector, aggregated_field
                        ));
                    } else {
                        let suffix = if function_name == "count" {
                            " != null"
                        } else {
                            ""
                        };

                        if table_alias.is_empty() {
                            linq_query.push_str(&format!(
                                "{} => {}.{}({} => {}{}.{}{})",
                                selector,
                                selector,
                                mapped_function_name,
                                self.row_selector,
                                cast,
                                self.row_selector,
                                &mapped_column.name,
                                suffix
                            ));
                        } else {
                            linq_query.push_str(&format!(
                                "{} => {}.{}({} => {}{}.{}.{}{})",
                                selector,
                                selector,
                                mapped_function_name,
                                self.row_selector,
                                cast,
                                self.row_selector,
                                table.mapped_alias,
                                &mapped_column.name,
                                suffix
                            ));
                        }
                    }
                } else {
                    panic!("Invalid function argument");
                }
            } else if let Expr::Identifier(ident) = &order_by.expr {
                let column_name = ident.to_string();

                let table_alias = self
                    .get_table_alias_from_field_name(&alias_to_table_map, &column_name)
                    .unwrap()
                    .to_string();

                let table = alias_to_table_map.get(&table_alias).unwrap();

                let mapped_column = self
                    .schema_mapping
                    .get_column(&table.name, &column_name)
                    .unwrap();

                let cast = if mapped_column.field_type == FieldType::Decimal {
                    "(double) "
                } else {
                    ""
                };

                if table_alias.is_empty() {
                    linq_query.push_str(&format!(
                        "{} => {}{}.{}",
                        selector, cast, selector, &mapped_column.name
                    ));
                } else {
                    linq_query.push_str(&format!(
                        "{} => {}{}.{}.{}",
                        selector, cast, selector, table.mapped_alias, &mapped_column.name
                    ));
                }
            } else if let Expr::CompoundIdentifier(ident) = &order_by.expr {
                let alias = ident[0].to_string();
                let field = ident[1].to_string();

                let table = alias_to_table_map.get(&alias).unwrap();

                let mapped_column = self.schema_mapping.get_column(&table.name, &field).unwrap();

                let cast = if mapped_column.field_type == FieldType::Decimal {
                    "(double) "
                } else {
                    ""
                };

                if group_by_fields.len() == 0 || group_by_fields.contains(&field) {
                    linq_query.push_str(&format!(
                        "{} => {}{}.{}.{}",
                        selector, cast, selector, table.mapped_alias, &mapped_column.name
                    ));
                } else {
                    linq_query.push_str(&format!(
                        "{} => {}{}.First().{}.{}",
                        selector, cast, selector, table.mapped_alias, &mapped_column.name
                    ));
                }
            } else if let Expr::BinaryOp { left, op, right } = &order_by.expr {
                let left_expr = self.build_where_expr(
                    left,
                    alias_to_table_map,
                    false,
                    &order_by.expr,
                    &HashMap::new(),
                );
                let right_expr = self.build_where_expr(
                    right,
                    alias_to_table_map,
                    false,
                    &order_by.expr,
                    &HashMap::new(),
                );
                let operator = match op {
                    BinaryOperator::Eq => " == ",
                    BinaryOperator::NotEq => " != ",
                    BinaryOperator::Gt => " > ",
                    BinaryOperator::GtEq => " >= ",
                    BinaryOperator::Lt => " < ",
                    BinaryOperator::LtEq => " <= ",
                    BinaryOperator::Minus => " - ",
                    _ => panic!("Unknown comparison operator"),
                };

                let expr = format!("{}{}{}", left_expr, operator, right_expr);

                let calculated_field = calculated_fields.get(&expr);

                if calculated_field.is_some() {
                    linq_query.push_str(&format!(
                        "{} => {}.{}",
                        selector,
                        selector,
                        calculated_field.unwrap(),
                    ));
                } else {
                    linq_query.push_str(&format!(
                        "{} => {}{}{}",
                        selector, left_expr, operator, right_expr
                    ));
                }
            } else {
                panic!("Unknown expression type");
            }

            linq_query.push_str(")");
        }

        return linq_query;
    }

    fn build_select(
        &self,
        select: &Box<Select>,
        use_new_object_for_select: bool,
        should_stringify_single_select: bool,
    ) -> SelectResult {
        let mut linq_query: HashMap<String, String> = HashMap::new();

        let main_table_name: String;
        let main_table_alias: String;
        let mapped_main_table_alias: String;

        let mut alias_to_table_map: CaseInsensitiveHashMap<TableAliasAndName> =
            CaseInsensitiveHashMap::new();

        let table = &select.from[0];

        if let sqlparser::ast::TableFactor::Table { name, alias, .. } = &table.relation {
            if let Some(alias) = alias {
                main_table_alias = alias.to_string();
                mapped_main_table_alias = alias.to_string();
            } else {
                if table.joins.len() > 0 {
                    main_table_alias = name.to_string();
                    mapped_main_table_alias = self
                        .schema_mapping
                        .get_table_name(&main_table_alias)
                        .unwrap()
                        .to_string();
                } else {
                    main_table_alias = "".to_string();
                    mapped_main_table_alias = "".to_string();
                }
            }

            main_table_name = name.to_string();

            alias_to_table_map.insert(
                main_table_alias.to_string(),
                TableAliasAndName {
                    mapped_alias: mapped_main_table_alias.to_string(),
                    name: main_table_name.to_string(),
                },
            );

            let mapped_table_name = self
                .schema_mapping
                .get_table_name(&main_table_name)
                .unwrap();

            linq_query.insert(
                "context".to_string(),
                format!("context.{}", mapped_table_name),
            );
        } else if let sqlparser::ast::TableFactor::Derived { subquery, .. } = &table.relation {
            let subquery_result = self.build_query_helper(
                subquery,
                Some(false),
                Some(false),
                Some(true),
                Some(false),
            );

            main_table_name = String::new();
            main_table_alias = String::new();

            linq_query.insert("context".to_string(), subquery_result);
        } else {
            panic!("Unknown table relation type");
        }

        if &table.joins.len() > &0 {
            linq_query.insert(
                "joins".to_string(),
                self.build_joins(table, &main_table_alias, &mut alias_to_table_map),
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

        let (group_by_query, group_by_fields) = self.build_group_by(select, &alias_to_table_map);

        linq_query.insert("group_by".to_string(), group_by_query);

        let selector = if has_group_by {
            &self.group_selector
        } else {
            &self.row_selector
        };

        linq_query.insert("selector".to_string(), selector.to_string());

        let mut calculated_fields: HashMap<String, String> = HashMap::new();
        let mut aggregated_fields: HashMap<String, String> = HashMap::new();
        if select.projection.len() > 0 {
            let mut current_linq_query = String::new();

            let has_only_one_select_item = select.projection.len() == 1;
            let first_select_item_is_unnamed =
                if let SelectItem::UnnamedExpr(_) = &select.projection[0] {
                    true
                } else {
                    false
                };
            let first_select_item_is_function =
                if let SelectItem::UnnamedExpr(expr) = &select.projection[0] {
                    if let Expr::Function(_) = expr {
                        true
                    } else {
                        false
                    }
                } else {
                    false
                };

            if !has_only_one_select_item
                || has_group_by
                || has_only_one_select_item
                    && first_select_item_is_unnamed
                    && !first_select_item_is_function
            {
                let projection_result = self.build_projection(
                    select,
                    &alias_to_table_map,
                    has_group_by,
                    &group_by_fields,
                    use_new_object_for_select,
                    should_stringify_single_select,
                );

                calculated_fields = projection_result.calculated_fields;
                aggregated_fields = projection_result.aggregated_fields;
                current_linq_query.push_str(&projection_result.select_result);

                if !has_group_by {
                    linq_query.insert("group_by".to_string(), projection_result.group_by_result);
                }
            } else {
                if let SelectItem::UnnamedExpr(expr) = &select.projection[0] {
                    let function = if let Expr::Function(function) = expr {
                        function
                    } else {
                        panic!("Unknown expression type");
                    };

                    let function_projection_result =
                        self.build_projection_single_function(function, &alias_to_table_map);

                    current_linq_query.push_str(&function_projection_result.result);

                    linq_query.insert(
                        "single_result_type".to_string(),
                        function_projection_result.field_type.to_string(),
                    );
                }
            }

            linq_query.insert("projection".to_string(), current_linq_query);
        }

        if select.selection.is_some() || select.having.is_some() {
            let (select_where, having_where) =
                self.build_where(select, &alias_to_table_map, &aggregated_fields);

            linq_query.insert("select_where".to_string(), select_where);

            // TODO: this could be done at the level of build_where
            for value in aggregated_fields.values() {
                if having_where.contains(&format!(".{} ", value)) {
                    linq_query.insert(
                        "having_where_is_using_aggregated_fields".to_string(),
                        "true".to_string(),
                    );
                }
            }

            linq_query.insert("having_where".to_string(), having_where);
        }

        if select.distinct.is_some() {
            linq_query.insert("distinct".to_string(), ".Distinct()".to_string());
        }

        if is_only_function {
        } else {
            linq_query.insert("final_aggregation".to_string(), ".ToList()".to_string());
        }

        return SelectResult {
            linq_query,
            alias_to_table_map,
            calculated_fields,
            group_by_fields,
            aggregated_fields,
        };
    }

    fn build_query_helper(
        &self,
        query: &Box<Query>,
        use_new_object_for_select: Option<bool>,
        with_semicolon: Option<bool>,
        skip_final_aggregation: Option<bool>,
        stringify_single_select: Option<bool>,
    ) -> String {
        let should_use_semicolon = if let Some(with_semicolon) = with_semicolon {
            with_semicolon
        } else {
            true
        };

        let should_skip_final_aggregation =
            if let Some(skip_final_aggregation) = skip_final_aggregation {
                skip_final_aggregation
            } else {
                false
            };

        let should_stringify_single_select =
            if let Some(stringify_single_select) = stringify_single_select {
                stringify_single_select
            } else {
                false
            };

        if let SetExpr::SetOperation {
            op, left, right, ..
        } = &*query.body
        {
            let left_select: HashMap<String, String>;
            let right_select: HashMap<String, String>;

            if let SetExpr::Select(select) = &**left {
                left_select = self.build_select(select, false, true).linq_query;
            } else {
                panic!("Unknown set expression type");
            }

            if let SetExpr::Select(select) = &**right {
                right_select = self.build_select(select, false, true).linq_query;
            } else {
                panic!("Unknown set expression type");
            }

            let left_query =
                self.build_result(&left_select, &HashMap::new(), false, true, false, false);
            let right_query =
                self.build_result(&right_select, &HashMap::new(), false, true, false, false);

            let mut result: String;
            if let sqlparser::ast::SetOperator::Intersect = op {
                result = format!("{}.Intersect({})", left_query, right_query);
            } else if let sqlparser::ast::SetOperator::Except = op {
                result = format!("{}.Except({})", left_query, right_query);
            } else if let sqlparser::ast::SetOperator::Union = op {
                if let Some(single_result_type) = left_select.get("single_result_type") {
                    result = format!(
                        "new List<{}> {{ {} }}.Union({})",
                        single_result_type, left_query, right_query
                    );
                } else {
                    result = format!("{}.Union({})", left_query, right_query);
                }
            } else {
                panic!("Unknown set operator");
            }

            let should_use_semicolon = if let Some(with_semicolon) = with_semicolon {
                with_semicolon
            } else {
                true
            };

            let should_skip_final_aggregation =
                if let Some(skip_final_aggregation) = skip_final_aggregation {
                    skip_final_aggregation
                } else {
                    false
                };

            if !should_skip_final_aggregation {
                result.push_str(".ToList()");
            }

            if should_use_semicolon {
                result.push_str(";");
            }

            return result;
        }

        let select = if let SetExpr::Select(select) = &*query.body {
            select
        } else {
            panic!("Unknown set expression type");
        };

        let should_use_new_object_for_select =
            if let Some(use_new_object) = use_new_object_for_select {
                use_new_object
            } else {
                true
            };

        let select_result = self.build_select(
            select,
            should_use_new_object_for_select,
            should_stringify_single_select,
        );

        let SelectResult {
            linq_query,
            alias_to_table_map,
            group_by_fields,
            calculated_fields,
            aggregated_fields,
        } = select_result;

        let selector = linq_query.get("selector").unwrap().to_string();
        let keywords_result = self.build_keywords(
            query,
            selector,
            &alias_to_table_map,
            &group_by_fields,
            &calculated_fields,
            &aggregated_fields,
        );

        let should_have_order_by_after_select =
            calculated_fields.len() > 0 || aggregated_fields.len() > 0;
        let should_have_having_after_select =
            linq_query.contains_key("having_where_is_using_aggregated_fields");

        return self.build_result(
            &linq_query,
            &keywords_result,
            should_use_semicolon,
            should_skip_final_aggregation,
            should_have_order_by_after_select,
            should_have_having_after_select,
        );
    }

    fn build_result(
        &self,
        select_result: &HashMap<String, String>,
        keywords_result: &HashMap<String, String>,
        with_semicolon: bool,
        skip_final_aggregation: bool,
        should_have_order_by_after_select: bool,
        should_have_having_after_select: bool,
    ) -> String {
        let mut linq_query = select_result.get("context").unwrap().to_string();

        append_if_some(&mut linq_query, select_result, "joins");
        append_if_some(&mut linq_query, select_result, "select_where");
        append_if_some(&mut linq_query, select_result, "group_by");

        if !should_have_having_after_select {
            append_if_some(&mut linq_query, select_result, "having_where");
        }

        if should_have_order_by_after_select {
            append_if_some(&mut linq_query, select_result, "projection");
            append_if_some(&mut linq_query, keywords_result, "order_by");
        } else {
            append_if_some(&mut linq_query, keywords_result, "order_by");
            append_if_some(&mut linq_query, select_result, "projection");
        }

        if should_have_having_after_select {
            append_if_some(&mut linq_query, select_result, "having_where");
        }

        append_if_some(&mut linq_query, select_result, "distinct");

        // The idea here is, if we have a single item in the projecton, then it doesn't make sense to limit it.
        // It's for stupid queries like `SELECT COUNT(*) FROM table LIMIT 2;`, for which limit is useless
        if !select_result.contains_key("single_result_type") {
            append_if_some(&mut linq_query, keywords_result, "limit");
        }

        if !skip_final_aggregation {
            append_if_some(&mut linq_query, select_result, "final_aggregation");
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
            return self.build_query_helper(query, None, None, None, None);
        } else {
            panic!("Unknown statement type");
        }
    }
}
