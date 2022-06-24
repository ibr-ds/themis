use std::{fmt::Debug, io::Read, str::FromStr};

use anyhow::{Context, Result};
use bytes::{Bytes, BytesMut};
use clap::Parser;
use serde::de::DeserializeOwned;
use tokio_util::codec::{Decoder, Encoder, LengthDelimitedCodec};

use themis_core::{
    app::{Client, Request},
    authentication::dummy::Dummy,
    net::{Message, MessageCodec, RawMessage, Tag},
};

fn validate_message<T>(bytes: &[u8]) -> Result<T>
where
    T: DeserializeOwned + Debug,
{
    let message: T = rmp_serde::from_slice(bytes)?;

    println!("Decoded Message: ");
    println!("{:#?}\n", message);
    Ok(message)
}

fn validate_length_delim<T: Tag>(mut stream: &mut BytesMut) -> Result<RawMessage<T>> {
    let mut message_codec = MessageCodec::<T>::new(Box::new(Dummy));

    let decoded = message_codec
        .decode(&mut stream)?
        .context("Incomplete Message")?;

    println!("Decoded Message: ");
    println!("{:#?}\n", decoded);

    Ok(decoded)
}

fn read(mut stream: impl Read) -> Result<BytesMut> {
    let mut vec = Vec::new();
    stream.read_to_end(&mut vec)?;
    Ok(BytesMut::from(vec.as_slice()))
}

fn demo(output_length: bool) -> Vec<u8> {
    let request = Request::new(111, Bytes::from_static(b"hello"));
    let request_bytes = rmp_serde::to_vec(&request).unwrap();
    println!(
        "1. Create a Request. Payload is arbitrary and interpreted only by the application: \n"
    );
    println!("{:#?}\n", request);

    let message = Message::new(222, 333, request);
    let mut message_packed = message.pack().unwrap();
    message_packed.signature = Bytes::from_static(b"signed");
    let bytes = rmp_serde::to_vec(&message_packed).unwrap();
    println!();
    println!("2. Wrap Request in Framework Header:");
    println!("{:#?}", message);

    println!();
    println!("3. Serialize the inner request first:");
    println!("{:#?}", message_packed);

    println!();
    println!("- Serialized request alone:");
    println!("{:?}", request_bytes);
    print_analyize(&request_bytes);

    println!();
    println!("4. Serialize the full message (3):");
    println!("{:?}", bytes);
    print_analyize(&bytes);

    let mut buf = BytesMut::new();
    let mut message_codec = MessageCodec::<Client>::new(Box::new(Dummy));
    message_codec.encode(message_packed, &mut buf).unwrap();

    println!(
        "5. To send the message over network, the total length of the message is preprended: \n"
    );
    println!("{:?}", buf.as_ref());

    if output_length {
        let mut vec = Vec::new();
        vec.extend(buf);
        vec
    } else {
        bytes
    }
}

fn print_analyize(bytes: &[u8]) {
    let base64 = base64::encode_config(&bytes, base64::STANDARD);
    println!(
        "Analyze serialized message: https://msgpack.dbrgn.ch/#base64={}",
        base64
    );
}

#[derive(Debug)]
enum HandshakeRole {
    Connector,
    Acceptor,
}

impl FromStr for HandshakeRole {
    type Err = anyhow::Error;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_ascii_lowercase().as_str() {
            "connector" => Ok(HandshakeRole::Connector),
            "acceptor" => Ok(HandshakeRole::Acceptor),
            _ => anyhow::bail!("unknown role"),
        }
    }
}

#[derive(Debug, Parser)]
enum Verbs {
    Validate {
        #[clap(long, short = 'l')]
        with_length: bool,
        #[clap(default_value = "-")]
        file: String,
    },
    Demo {
        #[clap(long, short = 'l')]
        with_length: bool,
        #[clap(long, default_value = "")]
        file: String,
    },
    Handshake {
        #[clap(default_value = "connector")]
        role: HandshakeRole,
        #[clap(long, default_value = "2864434397")]
        id: u64,
    },
}

#[derive(Debug, Parser)]
struct Opts {
    #[clap(subcommand)]
    verb: Verbs,
}

fn main() -> Result<()> {
    let opts = Opts::parse();

    match opts.verb {
        Verbs::Demo { file, with_length } => {
            let bytes = demo(with_length);
            if !file.is_empty() {
                std::fs::write(&file, bytes).context(file)?;
            }
        }
        Verbs::Validate { file, with_length } => {
            let mut bytes = if file == "-" {
                let stdin = std::io::stdin();
                let stdin = stdin.lock();
                read(stdin)?
            } else {
                let file = std::fs::File::open(&file).context(file)?;
                read(file)?
            };

            println!("Input: ");
            println!("{:?}\n", bytes.as_ref());

            let request: Message<Request> = if with_length {
                let message = validate_length_delim::<Client>(&mut bytes)?;
                message.unpack()?
            } else {
                let message = validate_message::<RawMessage<Client>>(&bytes)?;
                message.unpack()?
            };

            println!("Request:");
            println!("{:#?}\n", request);
        }
        Verbs::Handshake { role: _role, id } => {
            let mut buffer = BytesMut::new();
            let mut codec = LengthDelimitedCodec::new();

            codec
                .encode(Bytes::copy_from_slice(&id.to_le_bytes()), &mut buffer)
                .unwrap();

            println!("Handshake: ");
            println!("Message format: 4 Bytes Message Length (just like regular messages). 8 Bytes Id in Little Endian format.");
            println!("Id Message for Id {}: {:?}", id, buffer.as_ref());

            println!();
            println!("1. Client sends own Id.");
            println!("2. Replica reads client Id.");
            println!("3. Replica sends own Id.");
            println!("4. Client receives Replica Id.");
        }
    }

    Ok(())
}
