#!/bin/bash

DATABASE_FOLDER="../databases"
OUTPUT_FOLDER="Models"

if [ -d "$DATABASE_FOLDER" ]; then
  for db_file in $DATABASE_FOLDER/*.db; do
    db_name=$(basename "$db_file" .db)

    # if [ "$db_name" != "cre_Theme_park" ]; then
    #   continue
    # fi

    db_folder="$OUTPUT_FOLDER/$db_name"

    if [ -d "$db_folder" ] && [ "$(ls -A "$db_folder")" ]; then
      echo "The folder '$db_folder' already exists and is not empty. Skipping..."
      continue
    fi

    mkdir -p "$db_folder"

    dotnet ef dbcontext scaffold "Data Source=$db_file" Microsoft.EntityFrameworkCore.Sqlite -o "$db_folder"

    find "$db_folder" -type f -name "*Context.cs" | while read -r file; do
      grep -v '#warning To protect potentially sensitive information' "$file" > temp && mv temp "$file"
    done
  done
else
  echo "Error: The folder '$DATABASE_FOLDER' does not exist."
fi

