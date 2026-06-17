use std::sync::{Arc, RwLock};

use meilisearch_types::dynamic_search_rules::{
    DynamicSearchRule, DynamicSearchRules as LegacyDynamicSearchRules, RuleUid,
};
use meilisearch_types::heed;
use meilisearch_types::heed::types::{SerdeJson, Str};
use meilisearch_types::heed::{Database, Env, RwTxn, WithoutTls};
use meilisearch_types::index_uid::IndexUid;
use meilisearch_types::tasks::{DsrUpdate, KindWithContent, Task};

use crate::{IndexScheduler, Result, RoFeatures};

const NUMBER_OF_DATABASES: u32 = 1;

mod db_name {
    pub const DYNAMIC_SEARCH_RULES: &str = "dynamic-search-rules";
}

pub struct DynamicSearchRules<'a> {
    index_scheduler: &'a IndexScheduler,
}

impl<'a> DynamicSearchRules<'a> {
    // not fetching features in index_scheduler so that the caller can pass features instantiated once per request
    pub fn new(index_scheduler: &'a IndexScheduler, features: RoFeatures) -> Option<Self> {
        features.check_dynamic_search_rules("").is_ok().then_some(Self { index_scheduler })
    }

    pub fn add_or_update(
        &self,
        rule: DynamicSearchRule,
        custom_metadata: Option<String>,
    ) -> Result<Task> {
        let kind = KindWithContent::DsrUpdate(DsrUpdate::CreateOrUpdate(rule));
        wip::fixme!("add network");
        self.index_scheduler.register_with_custom_metadata(kind, None, custom_metadata, false, None)
    }

    pub fn delete(&self, rule_uid: RuleUid, custom_metadata: Option<String>) -> Result<Task> {
        let kind = KindWithContent::DsrUpdate(DsrUpdate::Deletion(rule_uid));
        wip::fixme!("add network");
        self.index_scheduler.register_with_custom_metadata(kind, None, custom_metadata, false, None)
    }
}

#[derive(Clone)]
pub(crate) struct DynamicSearchRulesStore {
    pub(crate) persisted: Database<Str, SerdeJson<DynamicSearchRule>>,
    runtime: Arc<RwLock<Arc<LegacyDynamicSearchRules>>>,
}

impl DynamicSearchRulesStore {
    pub(crate) const fn nb_db() -> u32 {
        NUMBER_OF_DATABASES
    }

    pub fn new(env: &Env<WithoutTls>, wtxn: &mut RwTxn) -> Result<Self> {
        let persisted = env.create_database(wtxn, Some(db_name::DYNAMIC_SEARCH_RULES))?;
        let rules: LegacyDynamicSearchRules = persisted
            .iter(wtxn)?
            .filter_map(|entry: Result<(&str, DynamicSearchRule), heed::Error>| {
                entry
                    .map(|(key, rule)| match key.parse::<IndexUid>() {
                        Ok(key) => Some((key, rule)),
                        Err(err) => {
                            tracing::error!("Error when deserializing from DB: {err}");
                            None
                        }
                    })
                    .transpose()
            })
            .collect::<Result<LegacyDynamicSearchRules, heed::Error>>()?;

        Ok(Self { persisted, runtime: Arc::new(RwLock::new(Arc::new(rules))) })
    }

    pub fn put(&self, mut wtxn: RwTxn, value: LegacyDynamicSearchRules) -> Result<()> {
        self.persisted.clear(&mut wtxn)?;
        for (uid, rule) in &value {
            self.persisted.put(&mut wtxn, uid, rule)?;
        }
        wtxn.commit()?;

        let mut runtime = self.runtime.write().unwrap();
        *runtime = Arc::new(value);
        Ok(())
    }

    pub fn get(&self) -> Arc<LegacyDynamicSearchRules> {
        self.runtime.read().unwrap().clone()
    }

    pub fn put_one(&self, wtxn: &mut RwTxn, rule: &DynamicSearchRule) -> Result<()> {
        self.persisted.put(wtxn, &rule.uid, rule)?;

        let mut lock = self.runtime.write().unwrap();
        Arc::make_mut(&mut lock).insert(rule.uid.clone(), rule.clone());
        Ok(())
    }

    pub fn delete_one(&self, wtxn: &mut RwTxn, uid: &RuleUid) -> Result<bool> {
        let deleted = self.persisted.delete(wtxn, uid)?;

        if deleted {
            let mut lock = self.runtime.write().unwrap();
            Arc::make_mut(&mut lock).remove(uid);
        }
        Ok(deleted)
    }
}
