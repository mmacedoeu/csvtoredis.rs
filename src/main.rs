extern crate csv;
extern crate redis;
#[macro_use] extern crate log;
extern crate env_logger;


use std::thread;
use std::sync::{Arc};
use std::sync::atomic::{AtomicBool, Ordering};
use redis::{Commands, PipelineCommands};
use std::path::PathBuf;
use std::collections::HashMap;
use log::LogLevel;

extern "C" {
  fn signal(sig: u32, cb: extern fn(u32));
}

extern fn interrupt(_:u32) {
	unsafe {
		match stop_loop {
		    Some(ref z) => z.store(true, Ordering::Relaxed),
		    None => {},
		}
	}
}

static mut stop_loop : Option<AtomicBool> = None;
const SERVER_UNIX_PATH: &'static str = "/tmp/redis.sock";
const REQUEST_KEY: &'static str = "csv.req";
const TIMEOUT: usize = 1;

fn get_client_addr() -> redis::ConnectionAddr {
	redis::ConnectionAddr::Unix(PathBuf::from(SERVER_UNIX_PATH))
}

fn parse_requests(item : &str) -> Option<(String, String)> {
	let mut rdr = csv::Reader::from_string(item).has_headers(false);
	let mut result : Option<(String, String)> = None;
	for row in rdr.decode() {
		let (cmd, target_key) : (String, String) = row.unwrap_or(break);
		let tuple = (cmd, target_key);
		result = Some(tuple);
		break;
	}
	result
}

fn init(target_key : String, process_map : &mut HashMap<String, Arc<AtomicBool>>, client : &redis::Client, fpath : &String) {
	let stop_process = Arc::new(AtomicBool::new(false));
	process_map.remove(&target_key);
	process_map.insert(target_key.clone(), stop_process.clone());
	let child_con = client.get_connection().unwrap();
	let fpathp = fpath.clone();
	let stopp = stop_process.clone();
	thread::spawn(|| {
		process(fpathp, target_key, stopp, child_con);
	});	
}

fn stop(target_key : String, process_map : &mut HashMap<String, Arc<AtomicBool>>) {
	match process_map.remove(&target_key) {
		Some(s) => {
			s.store(true, Ordering::Relaxed);
		},
		None => {},
	}	
}

fn handle_cmd(cmd : String, target_key : String, process_map : &mut HashMap<String, Arc<AtomicBool>>, client : &redis::Client, fpath : &String) {
	match cmd.as_ref() {
		"init" => init(target_key, process_map, client, fpath),
		"stop" => stop(target_key, process_map),
		_ => {},
	}
}

fn handle_requests(fpath : String, stop : &'static Option<AtomicBool>) -> redis::RedisResult<()> {
    // general connection handling
    //let client = redis::Client::open("redis://127.0.0.1/").unwrap();

    let client = redis::Client::open(redis::ConnectionInfo {
           addr: Box::new(get_client_addr()),
           db: 0,
           passwd: None,
    }).unwrap();

    let con = client.get_connection().unwrap();
    let mut process_map : HashMap<String, Arc<AtomicBool>> = HashMap::new();
    loop {
		unsafe {
			match stop_loop {
			    Some(ref z) => if z.load(Ordering::Relaxed) {break},
			    None => {},
			}
    	}

		let pop : Option<(String, String)> = try!(con.brpop(REQUEST_KEY, TIMEOUT));
		match pop {
		    None => {},			
		    Some((_, item)) => {
		    			info!("item");
		    			let req = parse_requests(item.as_ref());
		    			match req {
		    			    Some((cmd, target_key)) => handle_cmd(cmd, target_key, &mut process_map, &client, &fpath),
		    			    None => {},
		    			};
			},
		}
    }

	Ok(())
}

fn process (fpath : String, key: String, stop : Arc<AtomicBool>, con: redis::Connection) {
    let mut rdrp = csv::Reader::from_file(fpath).unwrap().has_headers(false).delimiter(b';').flexible(true);	 

	for row in rdrp.records() {
		let mut writer = csv::Writer::from_memory().delimiter(b';').flexible(true); 
    	let row = row.unwrap();

    	if row.len() == 36 {
    		let result = writer.encode(row);
    		assert!(result.is_ok());
    		//println!("{:?}", writer.as_string());

			let _ : () = con.lpush(key.clone(), writer.as_string()).unwrap();
		}
		if stop.load(Ordering::Relaxed) {break}
	}
}

fn main() {
	let fpath = ::std::env::args().nth(1).unwrap();
    env_logger::init().unwrap();
    
    unsafe {
    	stop_loop = Some(AtomicBool::new(false));
      	signal(2, interrupt);
    	let _ : () = handle_requests(fpath, &stop_loop).unwrap();      	
    }

}
