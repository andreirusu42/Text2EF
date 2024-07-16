use std::collections::HashMap;

use sqlparser::ast::{
    BinaryOperator, Expr, FunctionArg, FunctionArgExpr, FunctionArguments, GroupByExpr, Query,
    Select, SelectItem, SetExpr, Statement, TableWithJoins,
};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;

use super::schema_mapping::{create_schema_map, SchemaMapping};

pub struct LinqQueryBuilder {
    pub schema_mapping: SchemaMapping,
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
        use_new_object_for_select_when_single_field: bool,
    ) -> (String, String) {
        let mut select_result = String::new();
        let mut group_by_result = String::new();

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

        // If there is more than one function, we need to add the GroupBy(row => 1)

        if !has_group_by {
            // filter by Expr::Function in select.projection

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

        for select_item in &select.projection {
            if let SelectItem::UnnamedExpr(expr) = select_item {
                if let Expr::Identifier(identifier) = expr {
                    let column_name = identifier.to_string();

                    let table_alias = self
                        .get_table_alias_from_field_name(&tables_with_aliases_map, &column_name)
                        .unwrap();

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
                                select_fields
                                    .push(format!("{}.First().{}", selector, mapped_column_name));
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

                    let table_name = tables_with_aliases_map
                        .iter()
                        .find(|(_, v)| *v == &table_alias)
                        .unwrap()
                        .0;

                    let mapped_column_name = self
                        .schema_mapping
                        .get_column_name(&table_name, &column_name)
                        .unwrap();

                    if has_group_by {
                        if group_by_fields.contains(&mapped_column_name) {
                            select_fields.push(format!("{}.Key.{}", selector, mapped_column_name));
                        } else {
                            select_fields.push(format!(
                                "{}.First().{}.{}",
                                selector, table_alias, mapped_column_name
                            ));
                        }
                    } else {
                        select_fields.push(format!(
                            "{}.{}.{}",
                            selector, table_alias, mapped_column_name
                        ));
                    }
                } else if let Expr::Function(function) = expr {
                    let function_name = function.name.to_string().to_lowercase();

                    // TODO: this only works now because we haven't had Count(something), only Count(*)
                    if function_name == "count" {
                        select_fields.push(format!("Count = {}.Count()", selector));
                    } else if ["min", "max", "avg"].contains(&function_name.as_str()) {
                        let column_name: String;
                        let column_alias: String;

                        if let FunctionArguments::List(list) = &function.args {
                            if list.args.len() == 1 {
                                if let FunctionArg::Unnamed(ident) = &list.args[0] {
                                    if let FunctionArgExpr::Expr(expr) = ident {
                                        if let Expr::Identifier(ident) = expr {
                                            column_name = ident.to_string();
                                            column_alias = String::new();
                                        } else if let Expr::CompoundIdentifier(ident) = expr {
                                            column_alias = ident[0].to_string();
                                            column_name = ident[1].to_string();
                                        } else {
                                            panic!("Invalid function argument");
                                        }
                                    } else {
                                        panic!("Invalid function argument");
                                    }
                                } else {
                                    panic!("Invalid function argument");
                                }
                            } else {
                                panic!("Invalid number of arguments for Min/Max/Avg function");
                            }
                        } else {
                            panic!("Invalid function arguments");
                        };

                        let table_alias = self
                            .get_table_alias_from_field_name(&tables_with_aliases_map, &column_name)
                            .unwrap();

                        let table_name = tables_with_aliases_map
                            .iter()
                            .find(|(_, v)| *v == table_alias)
                            .unwrap()
                            .0;

                        let mapped_column_name = self
                            .schema_mapping
                            .get_column_name(&table_name, &column_name)
                            .unwrap();

                        let mapped_function_name = if function_name == "min" {
                            "Min"
                        } else if function_name == "max" {
                            "Max"
                        } else {
                            "Average"
                        };

                        let field_name = format!("{}{}", mapped_function_name, mapped_column_name);

                        if table_alias.is_empty() {
                            select_fields.push(format!(
                                "{} = {}.{}({} => {}.{})",
                                field_name,
                                selector,
                                mapped_function_name,
                                self.row_selector,
                                self.row_selector,
                                mapped_column_name,
                            ));
                        } else {
                            select_fields.push(format!(
                                "{} = {}.{}({} => {}.{}.{})",
                                field_name,
                                selector,
                                mapped_function_name,
                                self.row_selector,
                                self.row_selector,
                                column_alias,
                                mapped_column_name,
                            ));
                        }
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
        select_result.push_str(&select_fields.join(", "));

        if should_use_new {
            select_result.push_str(" })");
        } else {
            select_result.push_str(")");
        }

        return (select_result, group_by_result);
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
    ) -> Option<&String> {
        let mut result: Option<&String> = None;

        for (table_name, table_alias) in tables_with_aliases_map {
            let columns = self.schema_mapping.get_table_columns(table_name).unwrap();

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
        tables_with_aliases_map: &HashMap<String, String>,
    ) -> (String, String) {
        let mut selection_query = String::new();
        let mut having_query = String::new();

        if let Some(selection) = &select.selection {
            let where_clause =
                self.build_where_helper(selection, tables_with_aliases_map, None, false);
            selection_query = format!(".Where({} => {})", self.row_selector, where_clause);
        }

        if let Some(having) = &select.having {
            let having_clause =
                self.build_where_helper(having, tables_with_aliases_map, None, true);
            having_query = format!(".Where({} => {})", self.group_selector, having_clause);
        }

        return (selection_query, having_query);
    }

    fn build_where_helper(
        &self,
        expr: &Expr,
        tables_with_aliases_map: &HashMap<String, String>,
        parent_precedence: Option<i32>,
        is_having: bool,
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
                        tables_with_aliases_map,
                        Some(precedence),
                        is_having,
                    );
                    let right_condition = self.build_where_helper(
                        right,
                        tables_with_aliases_map,
                        Some(precedence),
                        is_having,
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
                    let left_condition =
                        self.build_where_expr(left, tables_with_aliases_map, is_having, expr);
                    let right_condition =
                        self.build_where_expr(right, tables_with_aliases_map, is_having, expr);
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
                if let Expr::Identifier(ident) = &**expr {
                    let field = ident.to_string();

                    let alias_option =
                        self.get_table_alias_from_field_name(tables_with_aliases_map, &field);

                    if alias_option.is_none() {
                        return field; // this happens when using double quotes on a string
                    }

                    let alias = alias_option.unwrap();

                    let table_name = tables_with_aliases_map
                        .iter()
                        .find(|(_, v)| *v == alias)
                        .unwrap()
                        .0;

                    let mapped_column_name = self
                        .schema_mapping
                        .get_column_name(&table_name, &field)
                        .unwrap();

                    let value = pattern.to_string();

                    return format!(
                        "EF.Functions.Like({}.{}, {})",
                        self.row_selector, mapped_column_name, value
                    );
                } else {
                    panic!("Unsupported expression type");
                }
            }
            Expr::InSubquery {
                subquery,
                expr,
                negated,
            } => {
                let built_subquery =
                    self.build_query_helper(subquery, Some(false), Some(false), Some(true));

                if let Expr::Identifier(ident) = &**expr {
                    let field = ident.to_string();

                    let alias_option =
                        self.get_table_alias_from_field_name(tables_with_aliases_map, &field);

                    let alias = alias_option.unwrap();

                    let table_name = tables_with_aliases_map
                        .iter()
                        .find(|(_, v)| *v == alias)
                        .unwrap()
                        .0;

                    let mapped_column_name = self
                        .schema_mapping
                        .get_column_name(&table_name, &field)
                        .unwrap();

                    let operator = if *negated { "!" } else { "" };

                    return format!(
                        "{}{}.Contains({}.{})",
                        operator, built_subquery, self.row_selector, mapped_column_name,
                    );
                } else {
                    panic!("Unsupported expression type");
                }
            }
            Expr::Between {
                low,
                high,
                expr,
                negated,
            } => {
                let low_condition =
                    self.build_where_expr(low, tables_with_aliases_map, is_having, expr);
                let high_condition =
                    self.build_where_expr(high, tables_with_aliases_map, is_having, expr);
                let expr_condition =
                    self.build_where_expr(expr, tables_with_aliases_map, is_having, expr);

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
        tables_with_aliases_map: &HashMap<String, String>,
        is_having: bool,
        root_expr: &Expr,
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
                format!("{}.{}.{}", selector, alias, mapped_column_name)
            }
            Expr::Identifier(ident) => {
                let field = ident.to_string();

                let alias_option =
                    self.get_table_alias_from_field_name(tables_with_aliases_map, &field);

                if alias_option.is_none() {
                    return field; // this happens when using double quotes on a string
                }

                let alias = alias_option.unwrap();

                let table_name = tables_with_aliases_map
                    .iter()
                    .find(|(_, v)| *v == alias)
                    .unwrap()
                    .0;

                let mapped_column_name = self
                    .schema_mapping
                    .get_column_name(&table_name, &field)
                    .unwrap();
                format!("{}.{}", selector, mapped_column_name)
            }
            Expr::Function(func) => {
                if func.name.to_string().to_lowercase() == "count" {
                    return format!("{}.Count()", selector);
                } else {
                    panic!("Unknown function");
                }
            }
            Expr::Value(value) => {
                let left = match &root_expr {
                    Expr::BinaryOp { left, .. } => left,
                    _ => return value.to_string(),
                };

                if let Expr::CompoundIdentifier(ident) = &**left {
                    let alias = ident[0].to_string();
                    let field = ident[1].to_string();

                    let table_name = tables_with_aliases_map
                        .iter()
                        .find(|(_, v)| *v == &alias)
                        .unwrap()
                        .0;

                    let mapped_column =
                        self.schema_mapping.get_column(&table_name, &field).unwrap();

                    if mapped_column.field_type == "bool" {
                        if let sqlparser::ast::Value::Number(num, ..) = value {
                            if num == "0" {
                                return "false".to_string();
                            } else {
                                return "true".to_string();
                            }
                        } else {
                            panic!("Unsupported value type");
                        }
                    }
                }

                return value.to_string().replace("'", "\"");
            }
            Expr::Subquery(subquery) => {
                let subquery_result =
                    self.build_query_helper(subquery, Some(false), Some(false), Some(true));

                return subquery_result;
            }
            _ => panic!("Unsupported expression type"),
        }
    }

    fn build_joins(
        &self,
        table: &TableWithJoins,
        tables_with_aliases_map: &mut HashMap<String, String>,
    ) -> String {
        let mut linq_query = String::new();
        let mut joined_aliases: Vec<String> = Vec::new();

        for join in &table.joins {
            linq_query.push_str(".Join(");

            let mapped_table_name: String;
            let table_alias: String;
            if let sqlparser::ast::TableFactor::Table {
                name,
                alias: Some(alias),
                ..
            } = &join.relation
            {
                let table_name = name.to_string();
                table_alias = alias.to_string();

                mapped_table_name = self
                    .schema_mapping
                    .get_table_name(&table_name)
                    .unwrap()
                    .to_string();

                tables_with_aliases_map.insert(table_name.to_string(), table_alias.to_string());

                linq_query.push_str(&format!("context.{}, ", mapped_table_name));
            } else {
                panic!("Unknown table factor type");
            }

            if let sqlparser::ast::JoinOperator::Inner(constraint) = &join.join_operator {
                if let sqlparser::ast::JoinConstraint::On(expr) = constraint {
                    if let sqlparser::ast::Expr::BinaryOp { left, right, .. } = expr {
                        let mut left_table_alias: String;
                        let mut left_table_field: String;

                        let mut right_table_alias: String;
                        let mut right_table_field: String;

                        if let Expr::CompoundIdentifier(ident) = &**left {
                            left_table_alias = ident[0].to_string();
                            left_table_field = ident[1].to_string();
                        } else {
                            panic!("Not a Compound Identifier");
                        }

                        if let Expr::CompoundIdentifier(ident) = &**right {
                            right_table_alias = ident[0].to_string();
                            right_table_field = ident[1].to_string();
                        } else {
                            panic!("Not a Compound Identifier");
                        }

                        if table_alias == left_table_alias {
                            let temp_table_alias = left_table_alias.clone();
                            let temp_table_field = left_table_field.clone();

                            left_table_alias = right_table_alias.clone();
                            left_table_field = right_table_field.clone();

                            right_table_alias = temp_table_alias;
                            right_table_field = temp_table_field;
                        }

                        let left_table_name = tables_with_aliases_map
                            .iter()
                            .find(|(_, v)| *v == &left_table_alias)
                            .unwrap()
                            .0;
                        let mapped_left_field = self
                            .schema_mapping
                            .get_column_name(&left_table_name, &left_table_field)
                            .unwrap();

                        let right_table_name = tables_with_aliases_map
                            .iter()
                            .find(|(_, v)| *v == &right_table_alias)
                            .unwrap()
                            .0;
                        let mapped_right_field = self
                            .schema_mapping
                            .get_column_name(&right_table_name, &right_table_field)
                            .unwrap();

                        let outer_key_selector: String;

                        if joined_aliases.len() == 0 {
                            outer_key_selector = left_table_alias.to_string();
                            joined_aliases.push(left_table_alias.clone());
                        } else {
                            outer_key_selector = "joined".to_string();
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
                                left_table_alias,
                                mapped_left_field
                            ));

                            linq_query.push_str(&format!(
                                "{} => {}.{}, ",
                                right_table_alias, right_table_alias, mapped_right_field
                            ));

                            linq_query.push_str(&format!(
                                "({}, {}) => new {{ {}{} }})",
                                outer_key_selector,
                                right_table_alias,
                                joined_aliases_str,
                                right_table_alias
                            ));
                        } else {
                            linq_query.push_str(&format!(
                                "{} => {}.{}, {} => {}.{}, ({}, {}) => new {{ {}, {} }})",
                                left_table_alias,
                                left_table_alias,
                                mapped_left_field,
                                right_table_alias,
                                right_table_alias,
                                mapped_right_field,
                                left_table_alias,
                                right_table_alias,
                                left_table_alias,
                                right_table_alias
                            ));
                        }

                        joined_aliases.push(right_table_alias.clone());
                    } else {
                        panic!("Unknown expression type");
                    }
                } else if let sqlparser::ast::JoinConstraint::None = constraint {
                    if let sqlparser::ast::TableFactor::Table { alias, .. } = &table.relation {
                        if let Some(alias) = alias {
                            let main_table_alias = alias.to_string();

                            return format!(
                                ".SelectMany({} => context.{}, ({}, {}) => new {{ {}, {} }})",
                                main_table_alias,
                                mapped_table_name,
                                main_table_alias,
                                table_alias,
                                main_table_alias,
                                table_alias
                            );
                        } else {
                            panic!("No alias for main table");
                        }
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

    fn build_keywords(
        &self,
        query: &Box<Query>,
        selector: String,
        tables_with_aliases_map: &HashMap<String, String>,
    ) -> HashMap<String, String> {
        let mut keywords: HashMap<String, String> = HashMap::new();

        if query.order_by.len() > 0 {
            keywords.insert(
                "order_by".to_string(),
                self.build_order_by(query, selector, tables_with_aliases_map),
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
            if let Expr::Value(value) = limit {
                linq_query.push_str(&format!("{}", value));
            } else {
                panic!("Unknown expression type");
            }
        }

        linq_query.push_str(")");

        return linq_query;
    }

    fn build_order_by(
        &self,
        query: &Box<Query>,
        selector: String,
        tables_with_aliases_map: &HashMap<String, String>,
    ) -> String {
        let mut linq_query = String::new();

        for order_by in &query.order_by {
            let is_ordering_asc = if let Some(is_ordering_asc) = order_by.asc {
                is_ordering_asc
            } else {
                false
            };

            if is_ordering_asc {
                linq_query.push_str(".OrderBy(");
            } else {
                linq_query.push_str(".OrderByDescending(");
            }

            if let Expr::Function(func) = &order_by.expr {
                let function_name = func.name.to_string().to_lowercase();

                if ["count", "avg", "sum"].contains(&function_name.as_str()) {
                    let args = if let FunctionArguments::List(list) = &func.args {
                        &list.args
                    } else {
                        panic!("Invalid function arguments");
                    };

                    if args.len() != 1 {
                        panic!("Invalid number of arguments for count/avg function");
                    }

                    let mapped_function_name = if function_name == "count" {
                        "Count"
                    } else if function_name == "sum" {
                        "Sum"
                    } else {
                        "Average"
                    };

                    if let FunctionArg::Unnamed(ident) = &args[0] {
                        if let FunctionArgExpr::Wildcard = ident {
                            linq_query.push_str(&format!(
                                "{} => {}.{}()",
                                selector, selector, mapped_function_name
                            ));
                        } else if let FunctionArgExpr::Expr(expr) = ident {
                            let column_name: String;
                            let table_alias: String;

                            if let Expr::Identifier(ident) = expr {
                                column_name = ident.to_string();
                                table_alias = self
                                    .get_table_alias_from_field_name(
                                        &tables_with_aliases_map,
                                        &column_name,
                                    )
                                    .unwrap()
                                    .to_string();
                            } else if let Expr::CompoundIdentifier(ident) = expr {
                                table_alias = ident[0].to_string();
                                column_name = ident[1].to_string();
                            } else {
                                panic!("Invalid function argument");
                            }

                            let table_name = tables_with_aliases_map
                                .iter()
                                .find(|(_, v)| *v == &table_alias)
                                .unwrap()
                                .0;

                            let mapped_column_name = self
                                .schema_mapping
                                .get_column_name(&table_name, &column_name)
                                .unwrap();

                            if table_alias.is_empty() {
                                linq_query.push_str(&format!(
                                    "{} => {}.{}({} => {}.{})",
                                    selector,
                                    selector,
                                    mapped_function_name,
                                    self.row_selector,
                                    self.row_selector,
                                    mapped_column_name
                                ));
                            } else {
                                linq_query.push_str(&format!(
                                    "{} => {}.{}({} => {}.{}.{})",
                                    selector,
                                    selector,
                                    mapped_function_name,
                                    self.row_selector,
                                    self.row_selector,
                                    table_alias,
                                    mapped_column_name
                                ));
                            }
                        } else {
                            panic!("Invalid function argument");
                        }
                    } else {
                        panic!("Invalid function argument");
                    }
                } else {
                    panic!("Unknown function");
                }
            } else if let Expr::Identifier(ident) = &order_by.expr {
                let column_name = ident.to_string();

                let table_alias = self
                    .get_table_alias_from_field_name(tables_with_aliases_map, &column_name)
                    .unwrap()
                    .to_string();

                let table_name = tables_with_aliases_map
                    .iter()
                    .find(|(_, v)| *v == &table_alias)
                    .unwrap()
                    .0;

                let mapped_column_name = self
                    .schema_mapping
                    .get_column_name(&table_name, &column_name)
                    .unwrap();

                if table_alias.is_empty() {
                    linq_query.push_str(&format!(
                        "{} => {}.{}",
                        selector, selector, mapped_column_name
                    ));
                } else {
                    linq_query.push_str(&format!(
                        "{} => {}.{}.{}",
                        selector, selector, table_alias, mapped_column_name
                    ));
                }
            } else if let Expr::CompoundIdentifier(ident) = &order_by.expr {
                let alias = ident[0].to_string();
                let field = ident[1].to_string();

                let table_name = tables_with_aliases_map
                    .iter()
                    .find(|(_, v)| *v == &alias)
                    .unwrap()
                    .0;

                let mapped_column_name = self
                    .schema_mapping
                    .get_column_name(table_name, &field)
                    .unwrap();

                linq_query.push_str(&format!(
                    "{} => {}.{}.{}",
                    selector, selector, alias, mapped_column_name
                ));
            } else {
                panic!("Unknown expression type");
            }
        }

        linq_query.push_str(")");

        return linq_query;
    }

    fn build_select(
        &self,
        select: &Box<Select>,
        use_new_object_for_select: bool,
    ) -> (HashMap<String, String>, HashMap<String, String>) {
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
        } else if let sqlparser::ast::TableFactor::Derived {
            lateral,
            subquery,
            alias,
        } = &table.relation
        {
            let subquery_result =
                self.build_query_helper(subquery, Some(false), Some(false), Some(true));

            main_table_name = String::new();

            linq_query.insert("context".to_string(), subquery_result);
        } else {
            panic!("Unknown table relation type");
        }

        if &table.joins.len() > &0 {
            linq_query.insert(
                "joins".to_string(),
                self.build_joins(table, &mut tables_with_aliases_map),
            );
        }

        if select.selection.is_some() || select.having.is_some() {
            let (select_where, having_where) = self.build_where(select, &tables_with_aliases_map);

            linq_query.insert("select_where".to_string(), select_where);

            linq_query.insert("having_where".to_string(), having_where);
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
                        let function_name = function.name.to_string().to_lowercase();

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
                                            // using .Distinct().[Func]() from below
                                        } else if let FunctionArgExpr::Expr(expr) = ident {
                                            let column_name: String;
                                            let table_alias: String;

                                            if let Expr::Identifier(ident) = expr {
                                                column_name = ident.to_string();
                                                table_alias = self
                                                    .get_table_alias_from_field_name(
                                                        &tables_with_aliases_map,
                                                        &column_name,
                                                    )
                                                    .unwrap()
                                                    .to_string();
                                            } else if let Expr::CompoundIdentifier(ident) = expr {
                                                table_alias = ident[0].to_string();
                                                column_name = ident[1].to_string();
                                            } else {
                                                panic!("Invalid function argument");
                                            }

                                            let table_name = tables_with_aliases_map
                                                .iter()
                                                .find(|(_, v)| *v == &table_alias)
                                                .unwrap()
                                                .0;

                                            let mapped_column_name = self
                                                .schema_mapping
                                                .get_column_name(&table_name, &column_name)
                                                .unwrap();

                                            if table_alias.is_empty() {
                                                current_linq_query.push_str(&format!(
                                                    ".Select({} => {}.{})",
                                                    self.row_selector,
                                                    self.row_selector,
                                                    mapped_column_name
                                                ));
                                            } else {
                                                current_linq_query.push_str(&format!(
                                                    ".Select({} => {}.{}.{})",
                                                    self.row_selector,
                                                    self.row_selector,
                                                    table_alias,
                                                    mapped_column_name
                                                ));
                                            }
                                        } else {
                                            panic!("Invalid function argument");
                                        }

                                        if is_distinct {
                                            current_linq_query.push_str(".Distinct()");
                                        }

                                        let mapped_function_name = if function_name == "count" {
                                            "Count"
                                        } else if function_name == "avg" {
                                            "Average"
                                        } else if function_name == "min" {
                                            "Min"
                                        } else if function_name == "max" {
                                            "Max"
                                        } else {
                                            "Sum"
                                        };

                                        current_linq_query
                                            .push_str(&format!(".{}()", mapped_function_name));
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
                    } else {
                        let (select_result, group_by_result) = self.build_projection(
                            select,
                            &tables_with_aliases_map,
                            &main_table_name,
                            has_group_by,
                            group_by_fields,
                            use_new_object_for_select,
                        );

                        current_linq_query.push_str(&select_result);

                        if !has_group_by {
                            linq_query.insert("group_by".to_string(), group_by_result);
                        }
                    }
                }
            } else {
                let (select_result, group_by_result) = self.build_projection(
                    select,
                    &tables_with_aliases_map,
                    &main_table_name,
                    has_group_by,
                    group_by_fields,
                    use_new_object_for_select,
                );

                current_linq_query.push_str(&select_result);

                if !has_group_by {
                    linq_query.insert("group_by".to_string(), group_by_result);
                }
            }

            linq_query.insert("projection".to_string(), current_linq_query);
        }

        // TODO: can be Distinct or Distinct On
        if select.distinct.is_some() {
            linq_query.insert("distinct".to_string(), ".Distinct()".to_string());
        }

        if is_only_function {
            // linq_query.insert("final_aggregation".to_string(), ".ToList()".to_string());
        } else {
            linq_query.insert("final_aggregation".to_string(), ".ToList()".to_string());
        }

        return (linq_query, tables_with_aliases_map);
    }

    fn build_query_helper(
        &self,
        query: &Box<Query>,
        use_new_object_for_select: Option<bool>,
        with_semicolon: Option<bool>,
        skip_final_aggregation: Option<bool>,
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

        if let SetExpr::SetOperation {
            op, left, right, ..
        } = &*query.body
        {
            let left_select: HashMap<String, String>;
            let right_select: HashMap<String, String>;

            if let SetExpr::Select(select) = &**left {
                left_select = self.build_select(select, false).0;
            } else {
                panic!("Unknown set expression type");
            }

            if let SetExpr::Select(select) = &**right {
                right_select = self.build_select(select, false).0;
            } else {
                panic!("Unknown set expression type");
            }

            let left_query = self.build_result(&left_select, &HashMap::new(), false, true);
            let right_query = self.build_result(&right_select, &HashMap::new(), false, true);

            let mut result: String;
            if let sqlparser::ast::SetOperator::Intersect = op {
                result = format!("{}.Intersect({})", left_query, right_query);
            } else if let sqlparser::ast::SetOperator::Except = op {
                result = format!("{}.Except({})", left_query, right_query);
            } else if let sqlparser::ast::SetOperator::Union = op {
                result = format!("{}.Union({})", left_query, right_query);
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

        let (select_result, tables_with_aliases_map) =
            self.build_select(select, should_use_new_object_for_select);
        let selector = select_result.get("selector").unwrap().to_string();
        let keywords_result = self.build_keywords(query, selector, &tables_with_aliases_map);

        return self.build_result(
            &select_result,
            &keywords_result,
            should_use_semicolon,
            should_skip_final_aggregation,
        );
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

        if let Some(select_where_clause) = select_result.get("select_where") {
            linq_query.push_str(select_where_clause);
        }

        if let Some(group_by) = select_result.get("group_by") {
            linq_query.push_str(group_by);
        }

        if let Some(having_where_clause) = select_result.get("having_where") {
            linq_query.push_str(having_where_clause);
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
            return self.build_query_helper(query, None, None, None);
        } else {
            panic!("Unknown statement type");
        }
    }

    //     pub fn create_test(&self, sql: &str, linq_query: &str) -> String {
    //         let mut test = String::new();

    //         test.push_str(&format!(
    //             r##"
    // public static void Test() {{
    //     using var context = new Activity1Context();

    //     var query = "{}";
    // }}
    //         "##,
    //             sql
    //         ));

    //         return test;
    //     }
}
