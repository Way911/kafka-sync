use figment::{
    providers::{Format, Toml},
    Figment,
};
use serde::{Deserialize, Serialize};
use std::sync::LazyLock;

use clap::Parser;

#[derive(Debug, Serialize, Deserialize)]
pub struct AppConfig {
    pub topic: String,
    pub src: KafkaSrc,
    pub dst: KafkaDst,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct KafkaSrc {
    pub brokers: Vec<String>,
    pub group_id: String,
    pub auto_offset_reset: String,
    pub enable_auto_commit: bool,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct KafkaDst {
    pub brokers: Vec<String>,
}

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
pub struct Cli {
    /// 配置文件路径 e.g. config.toml
    #[arg(short, long, default_value_t = String::from("config.toml"))]
    pub cfg_file_path: String,
    #[arg(short, long)]
    pub topic: Option<String>,
}

static CLI: LazyLock<Cli> = LazyLock::new(Cli::parse);

pub static CFG: LazyLock<AppConfig> = LazyLock::new(|| {
    let mut cfg = match Figment::new()
        .merge(Toml::file(&CLI.cfg_file_path))
        .extract::<AppConfig>()
    {
        Ok(cfg) => cfg,
        Err(err) => panic!("Error parsing cfg file {}, {}", &CLI.cfg_file_path, err),
    };

    if let Some(topic) = &CLI.topic {
        cfg.topic = topic.clone()
    }

    cfg
});
