pub mod cache;
pub mod discovery;
pub mod error;
pub mod filters;

use crate::amms::amm::AutomatedMarketMaker;
use crate::amms::amm::AMM;
use crate::amms::error::AMMError;
use crate::amms::factory::Factory;

use alloy::eips::BlockId;
use alloy::rpc::types::{Block, Filter, FilterSet, Header, Log};
use alloy::{
    network::Network,
    primitives::{Address, FixedBytes},
    providers::Provider,
};
use async_stream::stream;

use error::StateSpaceError;
use filters::AMMFilter;
use filters::PoolFilter;
use futures::stream::FuturesUnordered;
use futures::Stream;
use futures::StreamExt;
use std::collections::HashSet;
use std::pin::Pin;
use std::{collections::HashMap, marker::PhantomData, sync::Arc};
use tokio::sync::RwLock;
use tracing::info;

pub const CACHE_SIZE: usize = 250;

#[derive(Clone)]
pub struct StateSpaceManager<N, P> {
    pub state: Arc<RwLock<StateSpace>>,
    // discovery_manager: Option<DiscoveryManager>,
    pub block_filter: Filter,
    pub provider: P,
    phantom: PhantomData<N>,
    // TODO: add support for caching
}

impl<N, P> StateSpaceManager<N, P> {
    pub async fn subscribe(
        &self,
    ) -> Result<
        Pin<Box<dyn Stream<Item = Result<Vec<Address>, StateSpaceError>> + Send>>,
        StateSpaceError,
    >
    where
        P: Provider<N> + Clone + 'static,
        N: Network<HeaderResponse = Header>,
    {
        let provider = self.provider.clone();
        let state = self.state.clone();
        let base_filter = self.block_filter.clone();

        let block_stream = provider.subscribe_blocks().await?.into_stream();

        Ok(Box::pin(stream! {
            tokio::pin!(block_stream);

            while let Some(new_block) = block_stream.next().await {
                let prev_block = state.read().await.block.clone();

                if prev_block.hash == new_block.hash {
                    // 1. Handle no-op for duplicate blocks
                    continue;
                }

                // 2. Handle fatal reorg error before any network calls
                if new_block.number <= prev_block.number {
                    yield Err(StateSpaceError::ReorgNotSupported {
                        current_block: prev_block.number,
                        new_block: new_block.number,
                    });
                    break; // Terminate the stream
                }

                let filter = base_filter
                    .clone()
                    .from_block(prev_block.number + 1)
                    .to_block(new_block.number);
                let logs = provider.get_logs(&filter).await?;

                let affected_amms = state.write().await.sync(&logs, &new_block)?;

                yield Ok(affected_amms);
            }
        }))
    }
}

// TODO: Drop impl, create a checkpoint
#[derive(Debug, Default)]
pub struct StateSpaceBuilder<N, P> {
    pub provider: P,
    pub factories: Vec<Factory>,
    pub amms: Vec<AMM>,
    pub filters: Vec<PoolFilter>,
    phantom: PhantomData<N>,
    // TODO: add support for caching
}

impl<N, P> StateSpaceBuilder<N, P>
where
    N: Network<HeaderResponse = Header, BlockResponse = Block>,
    P: Provider<N> + Clone + 'static,
{
    pub fn new(provider: P) -> StateSpaceBuilder<N, P> {
        Self {
            provider,
            factories: vec![],
            amms: vec![],
            filters: vec![],
            // discovery: false,
            phantom: PhantomData,
        }
    }

    pub fn with_factories(self, factories: Vec<Factory>) -> StateSpaceBuilder<N, P> {
        StateSpaceBuilder { factories, ..self }
    }

    pub fn with_amms(self, amms: Vec<AMM>) -> StateSpaceBuilder<N, P> {
        StateSpaceBuilder { amms, ..self }
    }

    pub fn with_filters(self, filters: Vec<PoolFilter>) -> StateSpaceBuilder<N, P> {
        StateSpaceBuilder { filters, ..self }
    }

    pub async fn sync(self) -> Result<StateSpaceManager<N, P>, AMMError> {
        let block = self
            .provider
            .get_block(BlockId::latest())
            .await
            .map_err(|e| AMMError::TransportError(e))?
            .ok_or_else(|| AMMError::MissingBlockHeader)?
            .header;

        let chain_tip = BlockId::from(block.number);
        let factories = self.factories.clone();
        let mut futures = FuturesUnordered::new();

        let mut filter_set = HashSet::new();
        for factory in &self.factories {
            for event in factory.pool_events() {
                filter_set.insert(event);
            }
        }

        for amm in self.amms.iter() {
            for event in amm.sync_events() {
                filter_set.insert(event);
            }
        }

        let block_filter = Filter::new().event_signature(FilterSet::from(
            filter_set.into_iter().collect::<Vec<FixedBytes<32>>>(),
        ));
        let mut amm_variants = HashMap::new();
        for amm in self.amms.into_iter() {
            amm_variants
                .entry(amm.variant())
                .or_insert_with(Vec::new)
                .push(amm);
        }

        for factory in factories {
            let provider = self.provider.clone();
            let filters = self.filters.clone();

            let extension = amm_variants.remove(&factory.variant());
            futures.push(tokio::spawn(async move {
                let mut discovered_amms = factory.discover(chain_tip, provider.clone()).await?;

                if let Some(amms) = extension {
                    discovered_amms.extend(amms);
                }

                // Apply discovery filters
                for filter in filters.iter() {
                    if filter.stage() == filters::FilterStage::Discovery {
                        let pre_filter_len = discovered_amms.len();
                        discovered_amms = filter.filter(discovered_amms).await?;

                        info!(
                            target: "state_space::sync",
                            factory = %factory.address(),
                            pre_filter_len,
                            post_filter_len = discovered_amms.len(),
                            filter = ?filter,
                            "Discovery filter"
                        );
                    }
                }

                discovered_amms = factory.sync(discovered_amms, chain_tip, provider).await?;

                // Apply sync filters
                for filter in filters.iter() {
                    if filter.stage() == filters::FilterStage::Sync {
                        let pre_filter_len = discovered_amms.len();
                        discovered_amms = filter.filter(discovered_amms).await?;

                        info!(
                            target: "state_space::sync",
                            factory = %factory.address(),
                            pre_filter_len,
                            post_filter_len = discovered_amms.len(),
                            filter = ?filter,
                            "Sync filter"
                        );
                    }
                }

                Ok::<Vec<AMM>, AMMError>(discovered_amms)
            }));
        }

        let mut state_space = StateSpace::new(block);
        while let Some(res) = futures.next().await {
            let synced_amms = res??;

            for amm in synced_amms {
                state_space.state.insert(amm.address(), amm);
            }
        }

        // Sync remaining AMM variants
        for (_, remaining_amms) in amm_variants.drain() {
            for mut amm in remaining_amms {
                let address = amm.address();
                amm = amm.init(chain_tip, self.provider.clone()).await?;
                state_space.state.insert(address, amm);
            }
        }

        Ok(StateSpaceManager {
            state: Arc::new(RwLock::new(state_space)),
            block_filter,
            provider: self.provider,
            phantom: PhantomData,
        })
    }
}

#[derive(Debug, Default)]
pub struct StateSpace {
    pub state: HashMap<Address, AMM>,
    /// Header of the *last* block that has been fully applied.
    pub block: Header,
}

impl StateSpace {
    /// Create a fresh state space at `block`.
    pub fn new(block: Header) -> Self {
        Self {
            state: HashMap::new(),
            block,
        }
    }

    pub fn get(&self, address: &Address) -> Option<&AMM> {
        self.state.get(address)
    }

    pub fn get_mut(&mut self, address: &Address) -> Option<&mut AMM> {
        self.state.get_mut(address)
    }

    /// Applies all logs up to and including `tip_block`, returning the set of affected AMM addresses.
    pub fn sync(
        &mut self,
        logs: &[Log],
        tip_block: &Header,
    ) -> Result<Vec<Address>, StateSpaceError> {
        // 1. Check for reorgs or stale blocks. Fail immediately if detected.
        if tip_block.number <= self.block.number {
            // Allow reprocessing the exact same block hash, but nothing else.
            if tip_block.hash != self.block.hash {
                return Err(StateSpaceError::ReorgNotSupported {
                    current_block: self.block.number,
                    new_block: tip_block.number,
                });
            } else {
                // No-op: same block as before.
                return Ok(vec![]);
            }
        }

        // 2. Apply logs. No caching logic needed.
        let mut affected_addresses = HashSet::new();
        for log in logs {
            if let Some(amm) = self.state.get_mut(&log.address()) {
                affected_addresses.insert(amm.address());
                amm.sync(log)?;
            }
        }

        // 3. Update the block header to the new tip.
        self.block = tip_block.clone();

        Ok(affected_addresses.into_iter().collect())
    }
}

#[macro_export]
macro_rules! sync {
    // Sync factories with provider
    ($factories:expr, $provider:expr) => {{
        StateSpaceBuilder::new($provider.clone())
            .with_factories($factories)
            .sync()
            .await?
    }};

    // Sync factories with filters
    ($factories:expr, $filters:expr, $provider:expr) => {{
        StateSpaceBuilder::new($provider.clone())
            .with_factories($factories)
            .with_filters($filters)
            .sync()
            .await?
    }};

    ($factories:expr, $amms:expr, $filters:expr, $provider:expr) => {{
        StateSpaceBuilder::new($provider.clone())
            .with_factories($factories)
            .with_amms($amms)
            .with_filters($filters)
            .sync()
            .await?
    }};
}
