# DBSeeder

Seeds initial/dictionary/static data into the database.

Uses simple CSV with the predefined structure to seed data into DB.

Short QA:

Q: Why don't seed data via SQL and Flyway/Liquidbase/etc?                                                                                                                                                                         

A: Data changing via SQL is enough for you for the project to start, but after a while, it's not very convenient to analyze a lot of SQLs to understand the last data state
  

Q: It's much simpler for me to do the same via SQL

A: Maybe, but usually slightly simpler to create/update CSV than write SQL for the same


Q: Why INSERT/UPDATE only, but not DELETE?

A: Because DELETE is a more sensitive operation than others. So it's better to remove unnecessary rows from CSV and create an SQL script to remove the same rows from DB
  

## CSV structure


CSV for seeds should contain at least 2 lines.                                                                                                                                                                                    
                                                                                                                                                                                                                                    
  * 1st line: list of used columns in the table, delimited with **;** by default                                                                                                                                                            
  * 2nd line: seed settings, delimited with **;** by default                                                                                                                                                                            
  * 3rd line and further: table data with accordance to columns in 1st line                                                                                                                                                         
                                                                                                                                                                                                                                  

Lines besides the first two can be commented with the symbol **#** at the line start

Usually, column data is inserted/updated via parameters, but when you use **!!** before value, this data used in place, for example
```
key, value
table: test_table; keys: key                                                                                                                     
                                                                                                                                                                                                                                    
test key 1;test value 1
test key 2;!!test value 2
```

produces SQL:
```
INSERT INTO test_table (key, value) VALUES (?,?)
INSERT INTO test_table (key, value) VALUES (?,'test value 2')
```                                                                                                                                                                                                                      

### Seed settings

  * **table** - table name (example: table: my_table)                                                                                                                                                                               
  * **keys** - key columns, delimited with **,** (example: keys: col1, col2). Usually, it's not the primary key, especially when the key it's autoincremented column, but a column with value, which uniquely identifies a row in CSV                
  * **condition** - additional condition for table checking/referencing (example: condition: is_deleted = 0)                                                                                                                        
  * **action** - possible actions with CSV data (example: action: modify)                                                                                                                                                           
      * **ignore** - ignore this table, do nothing                                                                                                                                                                                  
      * **ignore_not_empty** - ignore this table only if the table has data, otherwise - insert data from CSV. Default                                                                                                                  
      * **insert** - check every row in table via **keys** and insert new row only                                                                                                                                                  
      * **modify** - check every row in table via **keys**, insert new rows or update existing                                                                                                                                      
  * **reference** - describe references to other tables (example: references: test_table_1_id = test_table_1(enum_field)). Usually used for foreign keys. Consists of 3 parts:                                                      
      * column name from 1st line                                                                                                                                                                                                   
      * referenced table                                                                                                                                                                                                            
      * referenced column                                                                                                                                                                                                           
  It means that for referencing column you should use values from a specified column from the referenced table and after processing this value must be replaced with the primary key value from the referenced table  
  
### Data description


  You can use data of any type from text value to dates and times (in ISO format). Data types should be recognized automatically.                                                                                                 

  For arrays, you should use **|** delimiter for array items.    
  
## Example of CSV file content

```
enum_field_2;big_field_2;read_only;test_array;test_array2;test_object;is_deleted;test_table_1_id                                                                                                                                    
table: test_table_2; keys: enum_field_2; references: test_table_1_id = test_table_1(enum_field); action: insert                                                                                                                     
                                                                                                                                                                                                                                    
TEST11;test_12;1;11 | 12 | 13;test_char 1 | test char 2 | test char 3;other test;1;TEST1                                                                                                                                            
TEST12;test_13;2;21 | 22 | 23;test_char 11 | test char 21 | test char 31;other test 11;0;TEST2
```   


## Usage in code

```
DBSSettings settings = new DBSSettings.Builder()                                                                                                                                                                            
    .connection(dbConnection) // DBConnection
    .dbSchema("PUBLIC") // database schema
    .sourceType(SourceType.CSV) // type of source data
    .sourceDir("data") // directory with source data (e.g. /src/main/resources/data)
    .build();                                                                                                                                                                                                           

DBSeeder seeder = new DBSeeder(settings);                                                                                                                                                                                   
seeder.read(); // read data from seed source
seeder.write(); // write data into DB
```
