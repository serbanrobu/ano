use std::{
    collections::{hash_map::Entry, HashMap},
    fmt,
    fs::{self, File},
    io::{self, prelude::*, BufRead, BufReader, Read, SeekFrom},
    ops::Deref,
    path::PathBuf,
    str::{self, FromStr},
};

use clap::Parser;
use color_eyre::eyre::{bail, ContextCompat, Result};
use fake::{
    faker::{
        address::en::{CityName, CountryName, SecondaryAddress, StateName, StreetName, ZipCode},
        finance::en::Bic,
        internet::en::SafeEmail,
        name::en::{FirstName, LastName, Name},
        phone_number::en::PhoneNumber,
        time::en::{Date, DateTime},
    },
    Fake, Faker,
};
use rand::Rng;
use serde_json::{json, Value};
use sqlparser::{dialect::MySqlDialect, parser::ParserError};
use thiserror::Error;
use tree_sitter::{Language, Node, Query, QueryCursor};

extern "C" {
    fn tree_sitter_sql() -> Language;
}

/// SQL anonymizer
#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Cli {
    /// SQL dump
    input: PathBuf,

    /// Tree query
    #[arg(short, long)]
    query: Option<PathBuf>,

    /// BufReader buffer size
    #[arg(short, long, default_value = "8192")]
    buffer_size: usize,
}

fn main() -> Result<()> {
    color_eyre::install()?;

    let cli = Cli::parse();
    let mut parser = tree_sitter::Parser::new();
    let language = unsafe { tree_sitter_sql() };
    parser.set_language(language).unwrap();
    let file = File::open(cli.input)?;
    let mut reader = BufReader::with_capacity(cli.buffer_size, file);

    let tree = parser
        .parse_with(
            &mut |byte, _| {
                reader.seek(SeekFrom::Start(byte as u64)).unwrap();
                reader.fill_buf().unwrap().to_vec()
            },
            None,
        )
        .unwrap();

    let root_node = tree.root_node();

    let query_source = if let Some(query) = cli.query {
        fs::read_to_string(query)?
    } else {
        String::new()
    };

    let query = Query::new(
        language,
        "(insert (object_reference name: (identifier) @_))",
    )?;

    let mut query_cursor = QueryCursor::new();
    let mut texts = HashMap::new();
    let query_captures = query_cursor.captures(&query, root_node, |_: Node| None.into_iter());

    for (query_match, _) in query_captures {
        for query_capture in query_match.captures {
            let node = query_capture.node;

            let Entry::Vacant(entry) = texts.entry(node.id()) else {
                continue;
            };

            let start_byte = node.start_byte();
            let end_byte = node.end_byte();
            reader.seek(SeekFrom::Start(start_byte as u64)).unwrap();
            let limit = end_byte - start_byte;
            let mut handle = reader.take(limit as u64);
            let mut buf = vec![0; limit];
            handle.read_exact(&mut buf).unwrap();
            reader = handle.into_inner();
            entry.insert(buf);
        }
    }

    let query = Query::new(language, &query_source)?;
    let capture_names = query.capture_names();

    let directives = capture_names
        .iter()
        .map(|n| n.parse::<Directive>().ok())
        .collect::<Vec<_>>();

    let mut query_cursor = QueryCursor::new();

    let query_matches = query_cursor.matches(&query, root_node, |node: Node| {
        texts.get(&node.id()).map(|v| &v[..]).into_iter()
    });

    let mut stdout = io::stdout().lock();
    let mut rng = rand::thread_rng();
    reader.rewind()?;

    for query_match in query_matches {
        for query_capture in query_match.captures {
            let Some(directive) = directives[query_capture.index as usize] else {
                continue;
            };

            let node = query_capture.node;
            let start_byte = node.start_byte() as u64;
            let pos = reader.stream_position()?;

            if pos < start_byte {
                let mut handle = reader.take(start_byte - pos);
                io::copy(&mut handle, &mut stdout)?;
                reader = handle.into_inner();
            }

            if pos > start_byte {
                bail!(
                    "unexpected capture node ({:?}) located before current position {}",
                    node.byte_range(),
                    pos
                );
            }

            reader = anonymize(directive, node, reader, &mut stdout, &mut rng)?;
        }
    }

    io::copy(&mut reader, &mut stdout)?;

    Ok(())
}

#[derive(Clone, Copy, Error, Debug)]
#[error("invalid directive")]
struct DirectiveError;

#[derive(Clone, Copy)]
enum Directive {
    Address,
    BiologicalSex,
    Bic,
    Date,
    Email,
    FirstName,
    Iban,
    LastName,
    Name,
    Order,
    Password,
    PhoneNumber,
    U32,
    VatNo,
}

impl FromStr for Directive {
    type Err = DirectiveError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "address" => Ok(Self::Address),
            "biological_sex" => Ok(Self::BiologicalSex),
            "bic" => Ok(Self::Bic),
            "date" => Ok(Self::Date),
            "email" => Ok(Self::Email),
            "first_name" => Ok(Self::FirstName),
            "iban" => Ok(Self::Iban),
            "last_name" => Ok(Self::LastName),
            "name" => Ok(Self::Name),
            "order" => Ok(Self::Order),
            "password" => Ok(Self::Password),
            "phone_number" => Ok(Self::PhoneNumber),
            "u32" => Ok(Self::U32),
            "vat_no" => Ok(Self::VatNo),
            _ => Err(DirectiveError),
        }
    }
}

fn anonymize(
    directive: Directive,
    node: Node,
    mut reader: BufReader<File>,
    writer: &mut impl Write,
    rng: &mut impl Rng,
) -> Result<BufReader<File>> {
    match directive {
        Directive::Address => {
            reader.seek(SeekFrom::Start(node.end_byte() as u64))?;

            let street_name: String = StreetName().fake_with_rng(rng);
            let street_details: String = SecondaryAddress().fake_with_rng(rng);
            let zip_code: String = ZipCode().fake_with_rng(rng);
            let city: String = CityName().fake_with_rng(rng);
            let country: String = CountryName().fake_with_rng(rng);
            let state: String = StateName().fake_with_rng(rng);

            let address = json!({
                "street_name": street_name,
                "street_details": street_details,
                "zip_code": zip_code,
                "city": city,
                "country": country,
                "state": state,
            });

            write!(writer, "{}", MySqlString(address.to_string()))?;
        }
        Directive::BiologicalSex => {
            reader.seek(SeekFrom::Start(node.end_byte() as u64))?;
            let biological_sex: &str = if Faker.fake() { "'Male'" } else { "'Female'" };
            write!(writer, "{}", biological_sex)?;
        }
        Directive::Bic => {
            reader.seek(SeekFrom::Start(node.end_byte() as u64))?;
            let bic: String = Bic().fake_with_rng(rng);
            write!(writer, "{}", MySqlString(bic))?;
        }
        Directive::Date => {
            reader.seek(SeekFrom::Start(node.end_byte() as u64))?;
            let date: String = Date().fake_with_rng(rng);
            write!(writer, "{}", MySqlString(date))?;
        }
        Directive::Email => {
            reader.seek(SeekFrom::Start(node.end_byte() as u64))?;
            let email: String = SafeEmail().fake_with_rng(rng);
            write!(writer, "{}", MySqlString(email))?;
        }
        Directive::FirstName => {
            reader.seek(SeekFrom::Start(node.end_byte() as u64))?;
            let first_name: String = FirstName().fake_with_rng(rng);
            write!(writer, "{}", MySqlString(first_name))?;
        }
        Directive::Iban => {
            reader.seek(SeekFrom::Start(node.end_byte() as u64))?;
            write!(writer, "'AT01234567890123456789'")?;
        }
        Directive::LastName => {
            reader.seek(SeekFrom::Start(node.end_byte() as u64))?;
            let last_name: String = LastName().fake_with_rng(rng);
            write!(writer, "{}", MySqlString(last_name))?;
        }
        Directive::Name => {
            reader.seek(SeekFrom::Start(node.end_byte() as u64))?;
            let name: String = Name().fake_with_rng(rng);
            write!(writer, "{}", MySqlString(name))?;
        }
        Directive::Order => {
            let limit = node.end_byte() - node.start_byte();
            let mut handle = reader.take(limit as u64);
            let mut buf = vec![0; limit];
            handle.read_exact(&mut buf)?;
            reader = handle.into_inner();
            let string: MySqlString = str::from_utf8(&buf)?.parse()?;

            let mut value: Value = serde_json::from_str(&string)?;

            let customer_details = value
                .as_object_mut()
                .wrap_err("not an object")?
                .get_mut("customerDetails")
                .wrap_err("undefined key `customerDetails`")?;

            let first_name: String = FirstName().fake_with_rng(rng);
            let last_name: String = LastName().fake_with_rng(rng);
            let email: String = SafeEmail().fake_with_rng(rng);
            let gender: &str = if Faker.fake() { "male" } else { "female" };
            let height: u32 = rng.next_u32();
            let weight: u32 = rng.next_u32();
            let birth_date: String = DateTime().fake_with_rng(rng);

            *customer_details = json!({
                "firstName": first_name,
                "lastName": last_name,
                "email": email,
                "gender": gender,
                "height": height,
                "weight": weight,
                "birthDate": birth_date,
            });

            write!(writer, "{}", MySqlString(value.to_string()))?;
        }
        Directive::Password => {
            reader.seek(SeekFrom::Start(node.end_byte() as u64))?;

            write!(
                writer,
                "'$2y$10$xOGO.s9/T06bIuCydNED7up5JWlXWp/kK7C8DC76kWyYrB5s9rnAu'"
            )?;
        }
        Directive::PhoneNumber => {
            reader.seek(SeekFrom::Start(node.end_byte() as u64))?;
            let phone_number: String = PhoneNumber().fake_with_rng(rng);
            write!(writer, "{}", MySqlString(phone_number))?;
        }
        Directive::U32 => {
            reader.seek(SeekFrom::Start(node.end_byte() as u64))?;
            write!(writer, "{}", rng.next_u32())?;
        }
        Directive::VatNo => {
            reader.seek(SeekFrom::Start(node.end_byte() as u64))?;
            write!(writer, "'AT01234567'")?;
        }
    }

    Ok(reader)
}

pub struct MySqlString(String);

impl FromStr for MySqlString {
    type Err = ParserError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        sqlparser::parser::Parser::new(&MySqlDialect {})
            .try_with_sql(s)?
            .parse_literal_string()
            .map(Self)
    }
}

impl Deref for MySqlString {
    type Target = String;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl fmt::Display for MySqlString {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "'{}'", self.0.as_bytes().escape_ascii())
    }
}
