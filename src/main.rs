#![feature(lazy_cell)]

use config::CFG;
use kafka::{
    consumer::{Consumer, FetchOffset, GroupOffsetStorage},
    producer::{AsBytes, Producer, Record, RequiredAcks},
};
mod config;
use std::thread;
use std::{sync::mpsc, time::Duration};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!(
        "topic: {:?}\nsrc: {:?}\ndst: {:?}",
        CFG.topic, CFG.src.brokers, CFG.dst.brokers
    );
    let offset = match CFG.src.auto_offset_reset.as_str() {
        "earliest" => FetchOffset::Earliest,
        "latest" => FetchOffset::Latest,
        _ => FetchOffset::Latest,
    };
    let mut con = Consumer::from_hosts(CFG.src.brokers.clone())
        .with_topic(CFG.topic.clone())
        .with_group(CFG.src.group_id.clone())
        .with_fallback_offset(offset)
        .with_offset_storage(Some(GroupOffsetStorage::Kafka))
        .create()?;

    let (tx, rx) = mpsc::channel();

    thread::spawn(move || {
        let mut producer = Producer::from_hosts(CFG.dst.brokers.clone())
            // ~ give the brokers one second time to ack the message
            .with_ack_timeout(Duration::from_secs(1))
            // ~ require only one broker to ack the message
            .with_required_acks(RequiredAcks::One)
            // ~ build the producer with the above settings
            .create()
            .unwrap();
        let mut batch_records: Vec<Record<'_, Vec<u8>, Vec<u8>>> = vec![];

        while let Ok(msg) = rx.recv() {
            batch_records.push(msg);
            if batch_records.len() >= 100 {
                send_batch(&mut producer, &batch_records).unwrap();
                batch_records.clear();
            }
        }

        // let received: Record<'_, Vec<u8>, Vec<u8>> = rx.recv().unwrap();
        // let key = from_utf8(&received.key).unwrap();
        // let val = from_utf8(&received.value).unwrap();
        // println!("key: {}, val: {}", key, val);
    });

    loop {
        let mss = con.poll()?;
        // if mss.is_empty() {
        //     println!("No messages available right now.");
        //     return Ok(());
        // }

        for ms in mss.iter() {
            for m in ms.messages() {
                let key = Vec::from(m.key);
                let val = Vec::from(m.value);
                // println!("key: {}, val: {}", key, val);
                let rcd = Record::from_key_value(&CFG.topic, key, val);
                tx.send(rcd)?;
                // batch_records.push(Record::from_key_value(
                //     &CFG.topic,
                //     m.key.clone(),
                //     m.value.clone(),
                // ));
            }
            let _ = con.consume_messageset(ms);
        }
        con.commit_consumed()?;
    }
}

fn send_batch<'a, K, V>(
    producer: &mut Producer,
    recs: &[Record<'a, K, V>],
) -> Result<(), kafka::error::Error>
where
    K: AsBytes,
    V: AsBytes,
{
    let rs = producer.send_all(recs)?;

    for r in rs {
        for tpc in r.partition_confirms {
            if let Err(code) = tpc.offset {
                return Err(kafka::Error::Kafka(code));
            }
        }
    }

    Ok(())
}
