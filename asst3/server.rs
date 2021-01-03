/*
 * server.rs
 *
 * Implementation of EasyDB database server
 *
 * University of Toronto
 * 2019
 */

use std::net::TcpListener;
use std::net::TcpStream;
use std::io::Write;
use std::io;
use packet::Command;
use packet::Response;
use packet::Network; 
use schema::Table;
use database;
use database::Database;
use std::thread;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicIsize, Ordering};

/*
fn single_threaded(listener: TcpListener, table_schema: Vec<Table>, verbose: bool)
{
    println!("Using Single-Threaded");
    /* 
     * you probably need to use table_schema somewhere here or in
     * Database::new 
     */
    // println!("{:?}", table_schema);   

    let mut db = Database::new(table_schema);

    for stream in listener.incoming() {
        let stream = stream.unwrap();
        
        if verbose {
            println!("Connected to {}", stream.peer_addr().unwrap());
        }
        
        match handle_connection(stream, &mut db) {
            Ok(()) => {
                if verbose {
                    println!("Disconnected.");
                }
            },
            Err(e) => eprintln!("Connection error: {:?}", e),
        };
    }
}
*/
fn multi_threaded(listener: TcpListener, table_schema: Vec<Table>, verbose: bool)
{
    println!("Using Multi-Threaded");
    let mut db = Arc::new(Mutex::new(Database::new(table_schema)));
    let mut atomic_int = Arc::new(Mutex::new(0));

    let mut thread_count = 0;

    for stream in listener.incoming() {

        let mut num = atomic_int.lock().unwrap();
        *num += 1;
        
        let mut stream = stream.unwrap();
        
        let db = Arc::clone(&db);
        let atomic_int = Arc::clone(&atomic_int);

        println!("Atomic Int: {}", num);
        println!("STREAM: {:?}", stream);

        if *num > 4 {
            println!("TOO MANY THREADS");
            *num -= 1;
            println!("Atomic Int: {}", num);
            stream.respond(&Response::Error(Response::SERVER_BUSY));
        }

        else if *num <= 4 {
            thread::spawn(move || {
                if verbose {
                    println!("Connected to {}", stream.peer_addr().unwrap());
                }
                
                match handle_connection(stream, db, atomic_int) {
                    Ok(()) => {
                        if verbose {
                            println!("Disconnected.");
                            // *num -= 1;
                        }
                    },
                    Err(e) => eprintln!("Connection error: {:?}", e),
                };
            });
        }
    }



}

/* Sets up the TCP connection between the database client and server */
pub fn run_server(table_schema: Vec<Table>, ip_address: String, verbose: bool)
{
    let listener = match TcpListener::bind(ip_address) {
        Ok(listener) => listener,
        Err(e) => {
            eprintln!("Could not start server: {}", e);
            return;
        },
    };
    
    println!("Listening: {:?}", listener);
    
    multi_threaded(listener, table_schema, verbose);
    //single_threaded(listener, table_schema, verbose);
}

impl Network for TcpStream {}

/* Receive the request packet from ORM and send a response back */
fn handle_connection(mut stream: TcpStream, db: std::sync::Arc<std::sync::Mutex<Database>>, atomic_int: std::sync::Arc<std::sync::Mutex<i32>>) 
    -> io::Result<()> 
{
    println!("Handle Connection");
    /* 
     * Tells the client that the connction to server is successful.
     * TODO: respond with SERVER_BUSY when attempting to accept more than
     *       4 simultaneous clients.
     */

    /*
    if thread_count > 4 {
        stream.respond(&Response::Error(Response::SERVER_BUSY))?;
        //return (Ok(()));
    }
    */
    
    stream.respond(&Response::Connected)?;

    loop {
        let request = match stream.receive() {
            Ok(request) => request,
            Err(e) => {
                /* respond error */
                stream.respond(&Response::Error(Response::BAD_REQUEST))?;
                return Err(e);
            },
        };
        
        /* we disconnect with client upon receiving Exit */
        if let Command::Exit = request.command {
            println!("THREAD DISCONNECTED");
            let mut num = atomic_int.lock().unwrap();
            *num -= 1;
            break;
        }
        
        let mut db = db.lock().unwrap();

        /* Send back a response */
        let response = database::handle_request(request, &mut db);
        
        stream.respond(&response)?;
        stream.flush()?;
    }

    Ok(())
}

