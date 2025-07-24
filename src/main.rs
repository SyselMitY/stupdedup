use std::{
    collections::{HashMap, HashSet},
    fs::{self, File},
    sync::{mpsc::channel, Arc, Mutex},
    thread::{self},
};

use clap::{Parser, ValueEnum};
use itertools::Itertools;
use rayon::prelude::*;
use walkdir::WalkDir;

const HASH_SEED: i64 = 0xBA647A7A;

#[derive(Parser)]
struct Args {
    #[arg(default_value = ".")]
    path: String,

    //Filter option
    #[arg(long = "filter")]
    filter: Option<Filter>,

    //print the duplicates
    #[arg(long)]
    print: bool,

    //do not use the file hash in determining duplicates
    #[arg(long)]
    nohash: bool,

    //print hash mismatches
    #[arg(long = "print-wrong-hash")]
    print_hash: bool,

    #[arg(long)]
    delete: bool,

    //logs reassignments of "original" files
    #[arg(long)]
    reassigns: bool,
}

#[derive(Clone, ValueEnum)]
enum Filter {
    //Ignore duplicates ending in " 2.xyz"
    Notwo,
    //Only duplicates ending in " 2.xyz"
    Onlytwo,
    //Only duplicates ending in " (2..9).xyz"
    Onlynum,
}

#[derive(Hash, PartialEq, Eq, Clone, Debug)]
struct FileInfo {
    name: String,
    name_undup: String,
    path: std::path::PathBuf,
    size: u64,
}

fn main() {
    let args = Args::parse();

    let files = read_fileinfos(&args.path);
    println!("File info collected, finding duplicates based on filename and size");
    let (originals, duplicates) = dedup_name_size(&files, args.reassigns);

    println!("Deduplication step 1 complete");
    println!("Filtering the results...");
    let filter = get_filter(&args.filter);

    let mut filtered = duplicates
        .into_iter()
        .filter(filter)
        //deref original to avoid double reference
        .map(|dup| (*originals.get(&dup.name_undup).unwrap(), dup))
        .into_group_map();
    println!("Filtered and grouped duplicates");

    if !args.nohash {
        println!("Checking file hashes");
        let discarded = check_hashes(&mut filtered, args.print_hash);
        //clear the map entries with empty duplicates
        filtered.retain(|_, dups| !dups.is_empty());
        println!("Discarded {discarded} false duplicates");
    }

    //Do things with the duplicates
    if args.print {
        filtered
            .iter()
            .sorted_by_key(|(orig, _dups)| &orig.path)
            .for_each(|(orig, dups)| {
                println!("{}", orig.path.display());
                dups.iter()
                    .for_each(|dup| println!("\t{}", dup.path.display()));
            });
    }

    let dup_count: usize = filtered
        .values() //duplicates vec
        .map(|dups| dups.len())
        .sum();

    println!("Found a total of {} duplicates.", dup_count);

    //delete
    if args.delete {
        filtered.iter().for_each(|(_, dups)| {
            dups.iter()
                .for_each(|dup| match std::fs::remove_file(&dup.path) {
                    Ok(_) => (),
                    Err(e) => println!("Error deleting file {}:\n{}", dup.path.display(), e),
                })
        });
    }
}

fn dedup_name_size(
    files: &HashSet<FileInfo>,
    print_reassigns: bool,
) -> (HashMap<String, &FileInfo>, HashSet<&FileInfo>) {
    let undup_name_map = Arc::new(Mutex::new(HashMap::new()));
    let duplicates = Arc::new(Mutex::new(HashSet::new()));

    files.par_iter().for_each(|fileinfo| {
        if fileinfo.name.contains("Na und-") {
            dbg!(fileinfo);
        }
        let mut undup_name_map = undup_name_map.lock().unwrap();

        let existing = undup_name_map.get(&fileinfo.name_undup);
        match existing {
            None => {
                undup_name_map.insert(fileinfo.name_undup.clone(), fileinfo);
            }
            Some(existing_entry) if existing_entry.size != fileinfo.size => (), //non duplicate
            Some(existing_entry) => {
                //prefer the "non postfixed" filename
                //CLONE performance penalty? maybe use RC instead
                if fileinfo.name.len() < existing_entry.name.len() {
                    //use fileinfo as new "original"
                    let mut duplicates = duplicates.lock().unwrap();
                    duplicates.insert(*existing_entry);
                    if print_reassigns {
                        println!(
                            "New original: {} (old: {})",
                            fileinfo.name, existing_entry.name
                        );
                    }
                    undup_name_map.insert(fileinfo.name_undup.clone(), fileinfo);
                    drop(undup_name_map);
                    duplicates.remove(&fileinfo);
                } else {
                    //fileinfo has longer name, so it is not considered the original
                    drop(undup_name_map);
                    let mut duplicates = duplicates.lock().unwrap();
                    duplicates.insert(fileinfo);
                }
            }
        }
    });

    //remove mutex and arc
    let duplicates = Arc::into_inner(duplicates).unwrap().into_inner().unwrap();
    let originals = Arc::into_inner(undup_name_map)
        .unwrap()
        .into_inner()
        .unwrap();

    (originals, duplicates)
}

fn read_fileinfos(path: &str) -> HashSet<FileInfo> {
    let mut count = 0;
    let iter = WalkDir::new(path)
        .into_iter()
        .filter_map(|e| match e {
            Ok(file) => Some(file),
            Err(e) => {
                println!("{}", e);
                None
            }
        })
        .filter(|e| e.file_type().is_file())
        .inspect(|_| {
            count += 1;
            print!("\rRead {} files", count);
        })
        .par_bridge()
        .filter_map(|entry| {
            let mut pathname = entry.path().to_path_buf();
            pathname.set_extension("");

            let name = pathname.file_name()?.to_string_lossy().into_owned();
            let undup_name = get_undestroyed_name(&name).to_string();
            let size = File::open(entry.path()).ok()?.metadata().unwrap().len();

            Some(FileInfo {
                name,
                name_undup: undup_name,
                size,
                path: entry.path().to_path_buf(),
            })
        });

    let result = iter.collect();
    println!();
    result
}

fn get_filter(filter_arg: &Option<Filter>) -> fn(&&FileInfo) -> bool {
    match filter_arg {
        Some(Filter::Notwo) => |file| !file.name.ends_with(" 2"),
        Some(Filter::Onlytwo) => |file| file.name.ends_with(" 2"),
        Some(Filter::Onlynum) => |file| {
            file.name.ends_with(" 2") || file.name.ends_with(" 3") || file.name.ends_with(" 4")
        },
        None => |_| true,
    }
}

fn get_undestroyed_name(name: &str) -> &str {
    if name.ends_with(" 2") || name.ends_with(" 3") || name.ends_with(" 4") {
        name.split_at(name.len() - 2).0
    } else if name.ends_with(" (1)") || name.ends_with(" (2)") || name.ends_with(" (3)") {
        name.split_at(name.len() - 4).0
    } else {
        name
    }
}

enum HashingUpdate {
    //an original was processed
    Completed,
    //nothing was completed, but reprint the line
    Refresh,
    //done
    Done,
}

//returns amount of false duplicates
fn check_hashes(dupmap: &mut HashMap<&FileInfo, Vec<&FileInfo>>, print: bool) -> u32 {
    let count = Arc::new(Mutex::new(0));
    let origs = dupmap.len();
    let (send, receive) = channel();

    thread::scope(|s| {
        s.spawn(move || {
            let mut processed = 0;
            let mut iter = receive.iter();
            loop {
                match iter.next() {
                    Some(HashingUpdate::Completed) => {
                        processed += 1;
                        print!("\rProcessed {processed} out of {origs} originals...");
                    }
                    Some(HashingUpdate::Refresh) => {
                        print!("\rProcessed {processed} out of {origs} originals...")
                    }
                    Some(HashingUpdate::Done) => {
                        println!();
                        break;
                    }
                    None => break, //what is going on?
                }
            }
        });
        dupmap.par_iter_mut().for_each(|(original, duplicates)| {
            //calc orig hash
            let orig_file = fs::read(&original.path);
            if let Ok(orig_file) = orig_file {
                //let orig_hash = seahash::hash(&orig_file);
                let orig_hash = gxhash::gxhash64(&orig_file, HASH_SEED);

                //check all duplicates
                duplicates.retain(|dup| match check_dup_hash(dup, orig_hash) {
                    false => {
                        if print {
                            println!("\rInvalid duplicate found: {}", dup.name);
                        }
                        let _ = send.send(HashingUpdate::Refresh);
                        let mut count = count.lock().unwrap();
                        *count += 1;
                        false
                    }
                    true => true,
                });
                //notify output thread
                let _ = send.send(HashingUpdate::Completed);
            }
        });
        //notify the display thread that were done
        let _ = send.send(HashingUpdate::Done);
    });

    Arc::into_inner(count).unwrap().into_inner().unwrap()
}

fn check_dup_hash(dup: &FileInfo, orig_hash: u64) -> bool {
    let dup_file = fs::read(&dup.path);
    if let Ok(dup_file) = dup_file {
        //let dup_hash = seahash::hash(&dup_file);
        let dup_hash = gxhash::gxhash64(&dup_file, HASH_SEED);

        return dup_hash == orig_hash;
    }
    //return false in case of error, since we cannot guarantee the duplicate
    false
}
