# PostgreSQL vacuumer script

This small Python script which uses Psycopg2 will examine the tables in the database, print out their sizes, issue VACUUM commands, and print out the new size and the duration of the vacuum, in a convenient table format.
