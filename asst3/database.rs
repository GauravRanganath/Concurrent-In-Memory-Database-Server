/*
 * database.rs
 *
 * Implementation of EasyDB database internals
 *
 * University of Toronto
 * 2019
 */

use packet::{Command, Request, Response, Value};
use schema::Table;
use std::collections::HashMap;
// use std::fmt::Display;
 
 
/* OP codes for the query command */
pub const OP_AL: i32 = 1;
pub const OP_EQ: i32 = 2;
pub const OP_NE: i32 = 3;
pub const OP_LT: i32 = 4;
pub const OP_GT: i32 = 5;
pub const OP_LE: i32 = 6;
pub const OP_GE: i32 = 7;

/* You can implement your Database structure here
 * Q: How you will store your tables into the database? */

// pub struct ValueData {
//     pub value_type: i32,     /* one of Value */
//     pub size: i32,          /* size of buf */
//     pub buf: String,        /* data stored within */
// }

// impl ValueData {
//     fn new(value_type: i32, size: i32, buf:String) -> ValueData {
//         ValueData {
//             value_type: value_type,
//             size: size,
//             buf: buf
//         }
//     }
// }

// pub struct Row {
//     pub count: i32,    /* number of elements in values */
//     pub values: Vec<ValueData>,
// }

// impl Row {
//     fn new(count: i32, values: Vec<ValueData>) -> Row {
//         Row {
//             count: count,
//             values: values,
//         }
//     }
// }

//------------

// pub struct ValueData {
//     pub values: Vec<Value>,
//     pub foreign_references: HashMap<i32, Vec<i64>>,  //HashMap<tableid, rowIds>
// }

// impl ValueData {
//     fn new(values: Vec<Value>, foreign_references: HashMap<i32, Vec<i64>>) -> ValueData {
//         ValueData {
//             values: values,
//             foreign_references: foreign_references,
//         }
//     }
// }

// pub struct Row {
//     pub fields: Vec<ValueData>,
//     pub version: i64,
// }

// impl Row {
//     fn new(fields: Vec<ValueData>, version: i64) -> Row {
//         Row {
//             fields: fields,
//             version: version,
//         }
//     }
// }

//------------------

pub struct Row {
    pub values: Vec<Value>,
    pub version: i64,
    pub valid: bool,    //if deleted or not
    // pub foreign_references: HashMap<i64, i32>,
    // pub back_foreign_references: Vec<(i32, i64)>,
    // pub forward_foreign_references: Vec<(i32, i64)>,
    pub references: Vec<(i32, i64)>,
}

impl Row {
    // fn new(values: Vec<Value>, version: i64, valid: bool, foreign_references: HashMap<i64, i32>) -> Row {
    // fn new(values: Vec<Value>, version: i64, valid: bool, back_foreign_references: Vec<(i32, i64)>, forward_foreign_references: Vec<(i32, i64)>, references: Vec<(i32, i64)>) -> Row {
    fn new(values: Vec<Value>, version: i64, valid: bool, references: Vec<(i32, i64)>) -> Row {

        // fn new(values: Vec<Value>, version: i64, valid: bool) -> Row {
        Row {
            values: values,
            version: version,
            valid: valid,
            // back_foreign_references: back_foreign_references,
            // forward_foreign_references: forward_foreign_references,
            references: references,
        }
    }
}

pub struct StoredTable {
    pub table_id: i32,
    pub columns: Vec<i32>,
    pub foreign_columns: Vec<i32>,   //col index of foreign val in values in row
    pub stored_data: HashMap<i64, Row>,    //<row id, row> <- can be a vector>
    // pub foreign_references: HashMap<i64, HashMap<i32,i64>>, //Directly realated rowIDs with index table_index
}

impl StoredTable {
    fn new(table_id: i32, columns:Vec<i32>, foreign_columns: Vec<i32>, stored_data: HashMap<i64,Row>) -> StoredTable {
        StoredTable {
            table_id: table_id,
            columns: columns,
            foreign_columns: foreign_columns,
            stored_data: stored_data,
            // foreign_references: foreign_references,
        }
    }
}

// impl fmt::Display for StoredTable {
//     fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
//         write!(f, "{}. {} ", self.table_id, self.stored_data)
//     }
// }

pub struct Database { 
    pub tables: Vec<StoredTable>,
}

impl Database {
    pub fn new(table_schema: Vec<Table>) -> Database {
        // println!("Making Database Object");
        
        let mut stored_tables: Vec<StoredTable> = Vec::new();
        for table in table_schema {
        
            let current_table_id = table.t_id;
            let mut col_types: Vec<i32> = Vec::new();
            let new_map: HashMap<i64, Row> = HashMap::new();
            let mut foreign_columns: Vec<i32> = Vec::new();
            // let foreign_references: HashMap<i64,HashMap<i32,i64>> = HashMap::new();

            // for col in table_schema[i].t_cols.iter() {
            //     match col.c_type {
            //         4 => foreign_columns.insert(col.c_id, ),
            //         _ => println!("Not a foreign"),
            //     };
            // }
            for col in table.t_cols.iter() {
                col_types.push(col.c_type);

                //Populate foreigns
                // println!("Col Type is {}", col.c_type);
                if col.c_type == 4 {
                    // println!("Table foreign col is in table {}", current_table_id);
                    // println!("This is a foreign");
                    // println!("Col id is {}", col.c_id);
                    // println!("Table id is {}", col.c_ref);
                    foreign_columns.push(col.c_ref);
                }
                // match col.c_type {
                //     // Value::INTEGER => String::from("integer"),
                //     // Value::FLOAT => String::from("float"),
                //     // Value::STRING => String::from("string"),
                //     4 => foreign_columns.insert(col.c_id, col.c_ref),
                //     _ => println!("Not a Foreign"),
                // }
            }

            let current_table = StoredTable::new(current_table_id, col_types, foreign_columns, new_map);
            stored_tables.push(current_table);

        }

        Database{
            tables: stored_tables,
        }
    }
}

// struct Data<T> {
//     value:T,
// }

/* Receive the request packet from client and send a response back */
pub fn handle_request(request: Request, db: & mut Database) 
    -> Response  
{           
    /* Handle a valid request */
    let result = match request.command {
        Command::Insert(values) => 
            handle_insert(db, request.table_id, values),
        Command::Update(id, version, values) => 
             handle_update(db, request.table_id, id, version, values),
        Command::Drop(id) => handle_drop(db, request.table_id, id),
        Command::Get(id) => handle_get(db, request.table_id, id),
        Command::Query(column_id, operator, value) => 
            handle_query(db, request.table_id, column_id, operator, value),
        /* should never get here */
        Command::Exit => Err(Response::UNIMPLEMENTED),
    };
    
    /* Send back a response */
    match result {
        Ok(response) => response,
        Err(code) => Response::Error(code),
    }
}

/*
 * TODO: Implment these EasyDB functions
 */
 
fn handle_insert(db: & mut Database, table_id: i32, values: Vec<Value>) 
    -> Result<Response, i32> 
{
    // println!("--------------------------------------ENTERING INSERT--------------------------------------");
    // println!("Into TableID {} : Inserting {:?}", table_id, values);
    // println!("Database is {:?}", db); 
    // println!("table_id is {}", table_id);
    // println!("values is {:?}", values);

    //Check for valid table_id
    if (table_id > db.tables.len() as i32) || table_id <= 0 {
        // println!("BAD_TABLE");
        return Err(Response::BAD_TABLE);
    }
    let table_index:usize = table_id as usize -1;

    //Check for valid amount of values
    if db.tables[table_index].columns.len() != values.len() {
        // println!("BAD_ROW");
        return Err(Response::BAD_ROW);
    }

    let curr_back_foreign_ref: Vec<(i32,i64)> = Vec::new();
    let curr_forward_foreign_ref: Vec<(i32,i64)> = Vec::new();
    let mut forward_foreigns: Vec<(i32,i64)> = Vec::new();
    let mut curr_ref: Vec<(i32,i64)> = Vec::new();


    let next_row_id:i64;
    if db.tables[table_index].stored_data.is_empty() {
        next_row_id = 1;
    }
    else{
        next_row_id = (db.tables[table_index].stored_data.keys().len() as i64) + 1;
    }

    // println!("From TableID {} : Inserting ObjectID {} {:?}", table_id, next_row_id, values);

    let row_version = 1;

    //Check column type mismatch
    for position in 0..db.tables[table_index].columns.len() {
        let col_type = db.tables[table_index].columns[position];
        // println!{"Position {} should be {}", position, col_type};
        let val_typ:i32 = 0;
        match &values[position] {
            Value::Null => {
                // println!("Field is Null");
            },
            Value::Integer(field_data) => if col_type != 1 {
                // println!("BAD_VALUE"); 
                return Err(Response::BAD_VALUE);
            },
            Value::Float(field_data) => if col_type != 2 {
                // println!("BAD_VALUE"); 
                return Err(Response::BAD_VALUE);
            },
            Value::Text(field_data) => if col_type != 3 {
                // println!("BAD_VALUE"); 
                return Err(Response::BAD_VALUE);
            },
            Value::Foreign(foreign_field) => {
                if col_type != 4 {
                    // println!("BAD_VALUE"); 
                    return Err(Response::BAD_VALUE);
                }
                // let colID = position +1;
                // println!("For Col id is {}", colID);
                let foreign_table_id:i32;
                match db.tables[table_index].foreign_columns.get(position) {
                    None => {
                        // println!("BAD_VALUEBAD"); 
                        return Err(Response::BAD_VALUE)
                    },
                    Some(val) => foreign_table_id = *val,
                }
                // println!("Foreign TableId is {:?}", foreign_table_id);

                //Check if the foriegn table has the row
                if !db.tables[foreign_table_id as usize -1].stored_data.contains_key(foreign_field){
                    // println!("BAD_FOREIGN");
                    return Err(Response::BAD_FOREIGN);
                }

                //Add this foreign reference to this row
                forward_foreigns.push((foreign_table_id, *foreign_field));

                // references.append(db.tables[foreign_table_id as usize -1].stored_data.get(foreign_field).unwrap().forward_foreign_references);
                // references.append(db.tables[foreign_table_id as usize -1].stored_data.get(foreign_field).unwrap().back_foreign_references);

                //Row in next table exists: add the reference to that row
                // println!("Adding ref to table id {} and ObjectID {}", foreign_table_id, foreign_field);
                // db.tables[foreign_table_id as usize -1].stored_data.get_mut(foreign_field).unwrap().back_foreign_references.push((table_id, next_row_id));
                db.tables[foreign_table_id as usize -1].stored_data.get_mut(foreign_field).unwrap().references.push((table_id, next_row_id));
                // println!("Source foreign: {:?}", db.tables[foreign_table_id as usize -1].stored_data.get_mut(foreign_field).unwrap().back_foreign_references);

                // let foreign_forward = db.tables[foreign_table_id as usize -1].stored_data.get_mut(foreign_field).unwrap().back_foreign_references
                // let foreign_back = 
                curr_ref.extend(&db.tables[foreign_table_id as usize -1].stored_data.get(foreign_field).unwrap().references);
                // println!("Current ref is {:?} ", curr_ref);

                // for i in 0..db.tables[foreign_table_id as usize -1].stored_data.get(foreign_field).unwrap().references.len() {
                //     let test = db.tables[foreign_table_id as usize -1].stored_data.get(foreign_field).unwrap().references[i];
                //     println!("JSJSJS To PUt in {:?}", test);
                //     curr_ref.push(test);
                // }
                // println!("NBNBNBNB table id {} and ObjectID {} has references {:?}", foreign_table_id, foreign_field, db.tables[foreign_table_id as usize -1].stored_data.get(foreign_field).unwrap().references );
                    
                for i in 0..db.tables[foreign_table_id as usize -1].stored_data.get(foreign_field).unwrap().references.len() {
                    let test = db.tables[foreign_table_id as usize -1].stored_data.get(foreign_field).unwrap().references[i];
                    if test.0 != table_id && test.0 != foreign_table_id {
                        // println!("Trying to add more ref to TableID {} with row id {}", test.0, test.1);
                        db.tables[test.0 as usize -1].stored_data.get_mut(&test.1).unwrap().references.extend(&curr_ref);
                        db.tables[test.0 as usize -1].stored_data.get_mut(&test.1).unwrap().references.push((foreign_table_id, *foreign_field));
                        db.tables[test.0 as usize -1].stored_data.get_mut(&test.1).unwrap().references.push((table_id, next_row_id));
                        // println!("Table ID {}, Object ID {} has ALL referneces {:?}", test.0, test.1, db.tables[test.0 as usize -1].stored_data.get(&test.1).unwrap().references);
                        
                    }
                } 
                

            },         
        }
    }
    // curr_ref.extend(&curr_back_foreign_ref);
    // curr_ref.extend(&curr_forward_foreign_ref);


    let current_row = Row::new(values, row_version, true, curr_ref);
    db.tables[table_index].stored_data.insert(next_row_id, current_row);

    // db.tables[table_index].stored_data.get_mut(&next_row_id).unwrap().forward_foreign_references.extend(&forward_foreigns);
    db.tables[table_index].stored_data.get_mut(&next_row_id).unwrap().references.extend(&forward_foreigns);
    // db.tables[table_index].stored_data.get_mut(&next_row_id).unwrap().references.extend(curr_ref);

    // println!("Table ID {}, Object ID {} has forward referneces {:?}", table_id, next_row_id, db.tables[table_index].stored_data.get_mut(&next_row_id).unwrap().forward_foreign_references);
    // println!("Table ID {}, Object ID {} has backward referneces {:?}", table_id, next_row_id, db.tables[table_index].stored_data.get_mut(&next_row_id).unwrap().back_foreign_references);
    // println!("Table ID {}, Object ID {} has ALL referneces {:?}", table_id, next_row_id, db.tables[table_index].stored_data.get_mut(&next_row_id).unwrap().references);

    // println!("--INSERT SUCCESSFUL--");
    // println!("Inserted into ObjectID {}", next_row_id);
    // println!("From TableID {} : Deleting ObjectID {} {:?}", table_id, next_row_id, values);
    Ok(Response::Insert(next_row_id, row_version))
}

fn handle_update(db: & mut Database, table_id: i32, object_id: i64, 
    version: i64, values: Vec<Value>) -> Result<Response, i32> 
{
    // println!("--ENTERING UPDATE--");
    // println!("table_id is {}", table_id);
    // println!("values is {:?}", values);

    //Check for valid table_id
    if (table_id > db.tables.len() as i32) || table_id <= 0 {
        // println!("BAD_TABLE");
        return Err(Response::BAD_TABLE);
    }
    let table_index:usize = table_id as usize -1;

    //Check for valid amount of values
    if db.tables[table_index].columns.len() != values.len() {
        // println!("BAD_ROW");
        return Err(Response::BAD_ROW);
    }

    //Check column type mismatch
    for (position, col_type) in db.tables[table_index].columns.iter().enumerate() {
        // println!{"Position {} should be {}", position, col_type};
        let val_typ:i32 = 0;
        match &values[position] {
            Value::Null => {
                // println!("Field is Null");
            },
            Value::Integer(field_data) => if *col_type != 1 {
                // println!("BAD_VALUE"); 
                return Err(Response::BAD_VALUE);},
            Value::Float(field_data) => if *col_type != 2 {
                // println!("BAD_VALUE"); 
                return Err(Response::BAD_VALUE);},
            Value::Text(field_data) => if *col_type != 3 {
                // println!("BAD_VALUE"); 
                return Err(Response::BAD_VALUE);},
            Value::Foreign(foreign_field) => {
                if *col_type != 4 {
                    // println!("BAD_VALUE"); 
                    return Err(Response::BAD_VALUE);
                }
                // let colID = position +1;
                // println!("For Col id is {}", colID);
                let foreign_table_id:i32;
                match db.tables[table_index].foreign_columns.get(position) {
                    None => {
                        // println!("BAD_VALUEBAD"); 
                        return Err(Response::BAD_VALUE)},
                    Some(val) => foreign_table_id = *val,
                }
                // println!("Foreign TableId is {:?}", foreign_table_id);

                //Check if the foriegn table has the row
                // println!("Foreign row is {}", foreign_field);
                // let foreign_row_id = foreign_row + &1;
                if !db.tables[foreign_table_id as usize -1].stored_data.contains_key(foreign_field){
                    // println!("BAD_FOREIGN");
                    return Err(Response::BAD_FOREIGN);
                }
            },         
        }
    }

    //Update Row if found
    match db.tables[table_index].stored_data.get(&object_id) {  
        None => {
            // println!("NOT_FOUND");
             return Err(Response::NOT_FOUND)},
        Some(row) => {
            // println!("Row Found is: {} {:?} and valid is {}", row.version, row.values, row.valid);
            if !(row.valid) {
                // println!("NOT_FOUND: DELETED"); 
                return Err(Response::NOT_FOUND);
            }
            if row.version != version && version != 0 {
                // println!("TXN_ABORT");
                return Err(Response::TXN_ABORT);
            }
            let new_version = row.version + 1;
            db.tables[table_index].stored_data.get_mut(&object_id).unwrap().values = values;     //Loop up alter() method
            db.tables[table_index].stored_data.get_mut(&object_id).unwrap().version = new_version;
            // println!("--UPDATE SUCCESSFUL--");
            return Ok(Response::Update(new_version));
        },
    }
}

fn handle_drop(db: & mut Database, table_id: i32, object_id: i64) 
    -> Result<Response, i32>
{
    // println!("--------------------------------------ENTERING DROP--------------------------------------");
    // println!("From TableID {} : Deleting ObjectID {:?}", table_id, object_id);
    // println!("Database is {:?}", db); 
    // println!("table_id is {}", table_id);
    // println!("object_is is {}", object_id);

    //Check for valid table_id
    if (table_id > db.tables.len() as i32) || table_id <= 0 {
        // println!("BAD_TABLE");
        return Err(Response::BAD_TABLE);
    }
    let table_index:usize = table_id as usize -1;

    //Check if object exists
    match db.tables[table_index].stored_data.get(&object_id) {  
        None => {
            // println!("NOT_FOUND");
            return Err(Response::NOT_FOUND);
        },
        Some(row) =>{
            // println!("To Drop is: {} {:?} and valid is {}", row.version, row.values, row.valid);
            if !(row.valid) {
                // println!("NOT_FOUND: DELETED"); 
                // return Err(Response::NOT_FOUND);
            }
            // println!("Backward References: {:?}", row.back_foreign_references);
            // println!("Forward References: {:?}", row.forward_foreign_references);
            
        },
    }

    if db.tables[table_index].stored_data.get(&object_id).unwrap().references.is_empty() {
        // println!("Tring to delete TableID {} and rowID {}", table_index, object_id);
        db.tables[table_index].stored_data.get_mut(&object_id).unwrap().valid = false;
        // println!("Deleted row : {:?}", db.tables[table_index].stored_data.get(&object_id).unwrap().values);

    }
    else {
        db.tables[table_index].stored_data.get_mut(&object_id).unwrap().references.sort_unstable();
        db.tables[table_index].stored_data.get_mut(&object_id).unwrap().references.dedup();
        // println!("ALL References: {:?}", db.tables[table_index].stored_data.get_mut(&object_id).unwrap().references);

        for to_delete in 0..db.tables[table_index].stored_data.get(&object_id).unwrap().references.len() {

            let reference = db.tables[table_index].stored_data.get(&object_id).unwrap().references[to_delete];
            // println!("Tring to delete TableID {} and rowID {}", reference.0, reference.1);
            db.tables[reference.0 as usize -1].stored_data.get_mut(&reference.1).unwrap().valid = false;
            // println!("Deleted row : {:?}", db.tables[reference.0 as usize -1].stored_data.get(&reference.1).unwrap().values);
        }
    }


    /*

    if db.tables[table_index].stored_data.get(&object_id).unwrap().forward_foreign_references.is_empty() &&
    db.tables[table_index].stored_data.get(&object_id).unwrap().back_foreign_references.is_empty() 
    {
        println!("This row has no forward foreigns - no backward foreign references");
        db.tables[table_index].stored_data.get_mut(&object_id).unwrap().valid = false;
        println!("1. Deleted row : {:?}", db.tables[table_index].stored_data.get(&object_id).unwrap().values);
        println!("--DROP SUCCESSFUL--");
        return Ok(Response::Drop);              
    }
    else if db.tables[table_index].stored_data.get(&object_id).unwrap().forward_foreign_references.is_empty() &&
    !(db.tables[table_index].stored_data.get(&object_id).unwrap().back_foreign_references.is_empty()) 
    {
        println!("This row has no forward foreigns - has backward foreign references");

        for i in 0..db.tables[table_index].stored_data.get(&object_id).unwrap().back_foreign_references.len() {
            let back_ref = db.tables[table_index].stored_data.get(&object_id).unwrap().back_foreign_references[i];
            println!("Current back ref: {:?}", back_ref);
            db.tables[back_ref.0 as usize -1].stored_data.get_mut(&back_ref.1).unwrap().forward_foreign_references.retain(|x| *x != (table_id, object_id));
            
            handle_drop(db, back_ref.0, back_ref.1);
        }
        db.tables[table_index].stored_data.get_mut(&object_id).unwrap().back_foreign_references.clear();
        db.tables[table_index].stored_data.get_mut(&object_id).unwrap().valid = false;

        // let reference = db.tables[table_index].stored_data.get(&object_id).unwrap().back_foreign_references[db.tables[table_index].stored_data.get(&object_id).unwrap().back_foreign_references.len() - 1];
        
        // db.tables[reference.0 as usize -1].stored_data.get_mut(&reference.1).unwrap().forward_foreign_references.retain(|x| *x != (table_id, object_id));
        
        // db.tables[table_index].stored_data.get_mut(&object_id).unwrap().back_foreign_references.pop();
        // handle_drop(db, reference.0, reference.1);
        // handle_drop(db, table_id, object_id);

    }
    else if !(db.tables[table_index].stored_data.get(&object_id).unwrap().forward_foreign_references.is_empty()) &&
    db.tables[table_index].stored_data.get(&object_id).unwrap().back_foreign_references.is_empty() 
    {
        println!("This row has forward foreigns - has no backward foreign references");

        for i in 0..db.tables[table_index].stored_data.get(&object_id).unwrap().forward_foreign_references.len() {
            let back_ref = db.tables[table_index].stored_data.get(&object_id).unwrap().forward_foreign_references[i];
            println!("Current back ref: {:?}", back_ref);
            db.tables[back_ref.0 as usize -1].stored_data.get_mut(&back_ref.1).unwrap().back_foreign_references.retain(|x| *x != (table_id, object_id));
            handle_drop(db, back_ref.0, back_ref.1);
        }
        db.tables[table_index].stored_data.get_mut(&object_id).unwrap().forward_foreign_references.clear();
        db.tables[table_index].stored_data.get_mut(&object_id).unwrap().valid = false;



        // let reference = db.tables[table_index].stored_data.get(&object_id).unwrap().forward_foreign_references[db.tables[table_index].stored_data.get(&object_id).unwrap().forward_foreign_references.len() - 1];
        
        // db.tables[reference.0 as usize -1].stored_data.get_mut(&reference.1).unwrap().back_foreign_references.retain(|x| *x != (table_id, object_id));

        // db.tables[table_index].stored_data.get_mut(&object_id).unwrap().forward_foreign_references.pop();
        // handle_drop(db, reference.0, reference.1);
        // handle_drop(db, table_id, object_id);

    }
    else if !(db.tables[table_index].stored_data.get(&object_id).unwrap().forward_foreign_references.is_empty()) &&
    !(db.tables[table_index].stored_data.get(&object_id).unwrap().back_foreign_references.is_empty()) {
        println!("This row has forward foreigns - has backward foreign references");

        for i in 0..db.tables[table_index].stored_data.get(&object_id).unwrap().back_foreign_references.len() {
            let back_ref = db.tables[table_index].stored_data.get(&object_id).unwrap().back_foreign_references[i];
            println!("Current back ref: {:?}", back_ref);
            db.tables[back_ref.0 as usize -1].stored_data.get_mut(&back_ref.1).unwrap().forward_foreign_references.retain(|x| *x != (table_id, object_id));
            handle_drop(db, back_ref.0, back_ref.1);
        }
        db.tables[table_index].stored_data.get_mut(&object_id).unwrap().back_foreign_references.clear();

        for i in 0..db.tables[table_index].stored_data.get(&object_id).unwrap().forward_foreign_references.len() {
            let back_ref = db.tables[table_index].stored_data.get(&object_id).unwrap().forward_foreign_references[i];
            println!("Current back ref: {:?}", back_ref);
            db.tables[back_ref.0 as usize -1].stored_data.get_mut(&back_ref.1).unwrap().back_foreign_references.retain(|x| *x != (table_id, object_id));
            handle_drop(db, back_ref.0, back_ref.1);
        }
        db.tables[table_index].stored_data.get_mut(&object_id).unwrap().forward_foreign_references.clear();
        db.tables[table_index].stored_data.get_mut(&object_id).unwrap().valid = false;


        
        // let reference_forward = db.tables[table_index].stored_data.get(&object_id).unwrap().forward_foreign_references[db.tables[table_index].stored_data.get(&object_id).unwrap().forward_foreign_references.len() - 1];
        
        // db.tables[table_index].stored_data.get_mut(&object_id).unwrap().forward_foreign_references.pop();
        // handle_drop(db, reference_forward.0, reference_forward.1); 
    }
    else {
        println!("Not possible");
    }

    */
    // println!("Exiting");
    return Ok(Response::Drop);    
}

fn handle_get(db: & Database, table_id: i32, object_id: i64) 
    -> Result<Response, i32>
{
    // println!("---------------------------------------ENTERING GET--------------------------------------");
    // println!("From TableID {} : Getting ObjectID {:?}", table_id, object_id);
    // println!("table_id is {}", table_id);
    // println!("object_id is {}", object_id);

    //Check for valid table_id
    if (table_id > db.tables.len() as i32) || table_id <= 0 {
        // println!("BAD_TABLE");
        return Err(Response::BAD_TABLE);
    }
    let table_index:usize = table_id as usize -1;

    //Get the row
    let version:i64;
    let values: Vec<Value>;
    match db.tables[table_index].stored_data.get(&object_id) {  
        None => {
            // println!("NOT_FOUND"); 
            return Err(Response::NOT_FOUND)
        },
        Some(row) => {
            // println!("Row Found is: {} {:?} and valid is {}", row.version, row.values, row.valid);
            if !(row.valid) {
                // println!("NOT_FOUND: DELETED"); 
                return Err(Response::NOT_FOUND);
            }

            // println!("--GET SUCCESSFUL--");
            return Ok(Response::Get(row.version, &row.values));
        },
    }
}

fn handle_query(db: & Database, table_id: i32, column_id: i32,
    operator: i32, other: Value) 
    -> Result<Response, i32>
{
    // println!("---------------------------------------ENTERING QUERY--------------------------------------");
    // println!("Table Id: {}, Column Id: {}, Operator: {}, Value: {:?}", table_id, column_id, operator, other);
    
    let mut table_index:usize = 0; 
    let mut col_index:usize = 0;

    if table_id > 0 {
        table_index = table_id as usize -1;
        // println!("Table Id: {}", table_id);
    }

    if column_id > 0 {
        col_index = column_id as usize -1;
        // println!("Col Id: {}", column_id);
    }

    let table = &db.tables[table_index];
        
    if table_id > db.tables.len() as i32 || table_id <= 0 {
        // println!("BAD_TABLE");
        return Err(Response::BAD_TABLE);
    }
    
    if (operator > 7 || operator < 1) {
        // println!("BAD_OPERATOR");
        return Err(Response::BAD_QUERY);
    }
    
    if (column_id > table.columns.len() as i32 || column_id < 0) {
        // println!("BAD_COLUMN_ID");
        return Err(Response::BAD_QUERY);
    } 
    
    // println!("Table Id: {}", table.table_id);
    // println!("Columns: {:?}, Column Length: {}", table.columns, table.columns.len());
    // println!("Foreign Columns: {:?}", table.foreign_columns);

    let mut response_row_ids: Vec<i64> = Vec::new();   

    if operator == 1 && column_id != 0 {
        // println!("INVALID AL OPERATOR");
        return Err(Response::BAD_QUERY);
    }

    if operator == 1 && column_id == 0 {
        // println!("TEST 1");
        for (key, val) in table.stored_data.iter() {
    
            response_row_ids.push(*key);
    
        }

        return Ok(Response::Query(response_row_ids));

    }

    // println!("TEST 2");

    if let Value::Foreign(ref other_val) = other {
        if operator > 3 {
            return Err(Response::BAD_QUERY);
        }
    }

    if let Value::Text(ref other_val) = other {
        if operator > 3 {
            return Err(Response::BAD_QUERY);
        }
    }
    
    for (key, val) in table.stored_data.iter() {
        // println!("key: {} val: {:?}", key, val.values);

        // println!("TEST 3");

        let scanned_val = & val.values[col_index];

        // println!("Column {} is {:?}", column_id, scanned_val);

        match &scanned_val {
            Value::Null => {
                // println!("NULL");
                if operator == 1 {
                    // println!("AL");
                    response_row_ids.push(*key);
                }
            }

            Value::Integer(scan_val) => {
                if let Value::Integer(other_val) = other {
                    // println!("Scanned Value: {}",scan_val);
                    // println!("Other Value: {}", other_val);
                    // println!("Row Id: {}", key);

                    if operator == 1 {
                        // println!("AL");
                        response_row_ids.push(*key);
                    }
    
                    else if operator == 2 {
                        // println!("EQ");
                        if *scan_val == other_val  {
                            response_row_ids.push(*key);
                        }
                    }
    
                    else if operator == 3 {
                        // println!("NE");
                        if *scan_val != other_val {
                            response_row_ids.push(*key);
                        }
                    }
    
                    else if operator == 4 {
                        // println!("LT");
                        if *scan_val < other_val {
                            response_row_ids.push(*key);
                        }
                    }
    
                    else if operator == 5 {
                        // println!("GT");
                        if *scan_val > other_val {
                            response_row_ids.push(*key);
                        }
                    }
    
                    else if operator == 6 {
                        // println!("LE");
                        if *scan_val <= other_val {
                            response_row_ids.push(*key);
                        }
                    }
    
                    else if operator == 7 {
                        // println!("GE");
                        if *scan_val >= other_val {
                            response_row_ids.push(*key);
                        }
                    }

                }
                else {
                    return Err(Response::BAD_QUERY);
                }
            }

            Value::Float(scan_val) => {
                if let Value::Float(other_val) = other {
                    // println!("Scanned Value: {}",scan_val);
                    // println!("Other Value: {}", other_val);
                    // println!("Row Id: {}", key);

                    if operator == 1 {
                        // println!("AL");
                        response_row_ids.push(*key);
                    }
    
                    else if operator == 2 {
                        // println!("EQ");
                        if *scan_val == other_val  {
                            response_row_ids.push(*key);
                        }
                    }
    
                    else if operator == 3 {
                        // println!("NE");
                        if *scan_val != other_val {
                            response_row_ids.push(*key);
                        }
                    }
    
                    else if operator == 4 {
                        // println!("LT");
                        if *scan_val < other_val {
                            response_row_ids.push(*key);
                        }
                    }
    
                    else if operator == 5 {
                        // println!("GT");
                        if *scan_val > other_val {
                            response_row_ids.push(*key);
                        }
                    }
    
                    else if operator == 6 {
                        // println!("LE");
                        if *scan_val <= other_val {
                            response_row_ids.push(*key);
                        }
                    }
    
                    else if operator == 7 {
                        // println!("GE");
                        if *scan_val >= other_val {
                            response_row_ids.push(*key);
                        }
                    }

                }
                else {
                    return Err(Response::BAD_QUERY);
                }
            }

            Value::Text(scan_val) => {
                if let Value::Text(ref other_val) = other {
                    // println!("Scanned Value: {}",scan_val);
                    // println!("Other Value: {}", other_val);
                    // println!("Row Id: {}", key);

                    let scanned_value: String = scan_val.to_string();
                    let other_value: String = other_val.to_string();

                    if operator == 1 {
                        // println!("AL");
                        response_row_ids.push(*key);
                    }
    
                    else if operator == 2 {
                        // println!("EQ");
                        if scanned_value == other_value {
                            response_row_ids.push(*key);
                        }
                    }
    
                    else if operator == 3 {
                        // println!("NE");
                        if scanned_value != other_value {
                            response_row_ids.push(*key);
                        }
                    }

                    else if operator > 3 {
                        // println!("Invalid Operator");
                        return Err(Response::BAD_QUERY);
                    }
                }
                else {
                    return Err(Response::BAD_QUERY);
                }
            }

            Value::Foreign(scan_val) => {
                if let Value::Foreign(ref other_val) = other {
                    // println!("Scanned Value: {}",scan_val);
                    // println!("Other Value: {}", other_val);
                    // println!("Row Id: {}", key);

                    if operator == 1 {
                        // println!("AL");
                        response_row_ids.push(*key);
                    }
    
                    else if operator == 2 {
                        // println!("EQ");
                        if scan_val == other_val {
                            response_row_ids.push(*key);
                        }
                    }
    
                    else if operator == 3 {
                        // println!("NE");
                        if scan_val != other_val {
                            response_row_ids.push(*key);
                        }
                    }

                    else if operator == 4 {
                        // println!("Invalid Operator");
                        return Err(Response::BAD_QUERY);
                    }
                }
                else {
                    return Err(Response::BAD_QUERY);
                }
            }
        }
    }

    // println!("Response: {:?}", response_row_ids);
    return Ok(Response::Query(response_row_ids));

}
