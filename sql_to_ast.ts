import { parse, show, cstVisitor, AllOtherClauses } from "sql-parser-cst";

const cst = parse("SELECT name AS x FROM Faculty;", {
  dialect: "sqlite",
  // These are optional:
  includeSpaces: true, // Adds spaces/tabs
  includeNewlines: true, // Adds newlines
  includeComments: true, // Adds comments
  includeRange: true, // Adds source code location data
});

const statement = cst.statements[0];

console.log(statement.type);
console.log(statement.range);
console.log(statement.leading);
console.log(statement.trailing);
