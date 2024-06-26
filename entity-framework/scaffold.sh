#!/bin/bash

DATABASE_FOLDER="../databases"
OUTPUT_FOLDER="Models"

if [ -d "$DATABASE_FOLDER" ]; then
  for db_file in $DATABASE_FOLDER/*.db; do
    db_name=$(basename "$db_file" .db)

    if [ "$db_name" != "activity_1" ]; then
      continue
    fi

    db_folder="$OUTPUT_FOLDER/$db_name"
    mkdir -p "$db_folder"

    dotnet ef dbcontext scaffold "Data Source=$db_file" Microsoft.EntityFrameworkCore.Sqlite -o "$db_folder"
  done
else
  echo "Error: The folder '$DATABASE_FOLDER' does not exist."
fi

