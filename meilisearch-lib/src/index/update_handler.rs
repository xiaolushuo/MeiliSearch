use milli::update::UpdateBuilder;
use milli::CompressionType;
use rayon::ThreadPool;

use crate::options::IndexerOpts;

pub struct UpdateHandler {
    max_nb_chunks: Option<usize>,
    chunk_compression_level: Option<u32>,
    thread_pool: ThreadPool,
    log_frequency: usize,
    max_memory: Option<usize>,
    chunk_compression_type: CompressionType,
}

impl UpdateHandler {
    pub fn new(opt: &IndexerOpts) -> anyhow::Result<Self> {
        let thread_pool = rayon::ThreadPoolBuilder::new()
            .num_threads(opt.indexing_jobs.unwrap_or(num_cpus::get() / 2))
            .build()?;

        Ok(Self {
            max_nb_chunks: opt.max_nb_chunks,
            chunk_compression_level: opt.chunk_compression_level,
            thread_pool,
            log_frequency: opt.log_every_n,
            max_memory: opt.max_memory.map(|m| m.get_bytes() as usize),
            chunk_compression_type: opt.chunk_compression_type,
        })
    }

    pub fn update_builder(&self) -> UpdateBuilder {
        // We prepare the update by using the update builder.
        let mut update_builder = UpdateBuilder::new();
        if let Some(max_nb_chunks) = self.max_nb_chunks {
            update_builder.max_nb_chunks(max_nb_chunks);
        }
        if let Some(chunk_compression_level) = self.chunk_compression_level {
            update_builder.chunk_compression_level(chunk_compression_level);
        }
        update_builder.thread_pool(&self.thread_pool);
        update_builder.log_every_n(self.log_frequency);
        if let Some(max_memory) = self.max_memory {
            update_builder.max_memory(max_memory);
        }
        update_builder.chunk_compression_type(self.chunk_compression_type);
        update_builder
    }
}
