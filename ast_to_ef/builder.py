from sql_to_ast.select_ast_builder import SelectAstBuilder

from ast_to_ef.transformers import from_clause_transformer, select_clause_transformer, group_by_clause_transformer, where_clause_transformer


def build_ef(sql: str):
    ast = SelectAstBuilder.build(sql)

    select_text = select_clause_transformer.build_select(ast.select_clause)
    from_text = from_clause_transformer.build_from(ast.from_clause)
    where_text = where_clause_transformer.build_where(ast.where_clause) if ast.where_clause else None
    group_by_text = group_by_clause_transformer.build_group_by(ast.group_by_clause) if ast.group_by_clause else None

    text = f"var result = {from_text}"

    if where_text:
        text += where_text

    if group_by_text:
        text += group_by_text

    text += select_text

    return text


def main():
    # sql = f"""
    # SELECT T1.fname ,  T1.lname FROM Faculty AS T1 JOIN Student AS T2 ON T1.FacID  =  T2.advisor WHERE T2.fname  =  'Linda' AND T2.lname  = 'Smith'
    # """

    # sql = f"""
    # SELECT count(*) FROM Has_allergy AS T1 JOIN Allergy_type AS T2 ON T1.allergy  =  T2.allergy WHERE T2.allergytype  =  'food'
    # """

    # sql = f"""
    # SELECT DISTINCT T2.Model
    # FROM CAR_NAMES AS T1
    # JOIN MODEL_LIST AS T2 ON T1.Model  =  T2.Model
    # JOIN CAR_MAKERS AS T3 ON T2.Maker  =  T3.Id
    # JOIN CARS_DATA AS T4 ON T1.MakeId  =  T4.Id
    # WHERE T3.FullName  =  'General Motors' OR T4.weight  >  3500
    # """

    # sql = f"""
    # SELECT COUNT(*) FROM country AS T1 JOIN countrylanguage AS T2 ON T1.Code  =  T2.CountryCode GROUP BY T1.Name
    # """

    sql = f"""
SELECT DISTINCT T2.Name FROM country AS T1 JOIN city AS T2 ON T2.CountryCode  =  T1.Code WHERE T1.Continent  =  'Europe' AND T1.Name NOT IN (SELECT T3.Name FROM country AS T3 JOIN countrylanguage AS T4 ON T3.Code  =  T4.CountryCode WHERE T4.IsOfficial  =  'T' AND T4.Language  =  'English')    """

    ef = build_ef(sql)

    print(ef)


if __name__ == "__main__":
    main()
