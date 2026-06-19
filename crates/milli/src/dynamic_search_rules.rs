use std::ops::Bound;

use heed::{RoTxn, WithoutTls};
use itertools::Itertools as _;
use roaring::RoaringBitmap;
use serde::Deserialize;
use time::format_description::well_known::Rfc3339;

use crate::heed_codec::facet::{FacetGroupKey, FacetGroupValue};
use crate::search::facet::ascending_facet_sort;
use crate::search::facet::facet_range_search::find_docids_of_facet_within_bounds;
use crate::search::new::LocatedQueryTerm;
use crate::update::new::document::DocumentFromDb;
use crate::{DocumentId, FieldsIdsMap, Index, PinDoc, Result, SearchContext, MAX_COUNTED_WORDS};

type RuleId = u32;

/// Wrapper around the DSR index, allowing to search for active rules
pub struct DynamicSearchRules {
    index: Index,
    rtxn: RoTxn<'static, WithoutTls>,
    db_fields_ids_map: FieldsIdsMap,
}

impl DynamicSearchRules {
    pub fn new(index: Index) -> Result<Self> {
        let rtxn = index.static_read_txn()?;

        let db_fields_ids_map = index.fields_ids_map(&rtxn)?;
        Ok(Self { index, rtxn, db_fields_ids_map })
    }

    pub fn get<'t>(&'t self, rule_uid: &str) -> Result<Option<DocumentFromDb<'t, FieldsIdsMap>>> {
        let Some(docid) = self.index.external_documents_ids().get(&self.rtxn, rule_uid)? else {
            return Ok(None);
        };

        let Some(doc) =
            DocumentFromDb::new(docid, &self.rtxn, &self.index, &self.db_fields_ids_map)?
        else {
            return Ok(None);
        };

        Ok(Some(doc))
    }

    pub fn resolve_pins(
        &self,
        query_terms: &[LocatedQueryTerm],
        universe: &mut RoaringBitmap,
        search_context: &SearchContext,
    ) -> Result<Vec<PinDoc>> {
        let active_rules = self.active_rules(query_terms, search_context)?;

        self.find_pins(self.rule_ids_sorted_by_precedence(active_rules)?, search_context)
            .filter(
                |pin| {
                    if let Ok(pin) = pin.as_ref() {
                        universe.remove(pin.doc_id)
                    } else {
                        true
                    }
                },
            )
            .collect()
    }

    fn find_pins<'a>(
        &'a self,
        sorted_active_rules: impl IntoIterator<Item = Result<RuleId>> + 'a,
        search_context: &'a SearchContext,
    ) -> impl Iterator<Item = Result<PinDoc>> + 'a {
        sorted_active_rules
            .into_iter()
            .map(|rule_id| {
                let rule_id = rule_id?;
                let Some(rule) =
                    DocumentFromDb::new(rule_id, &self.rtxn, &self.index, &self.db_fields_ids_map)?
                else {
                    tracing::warn!(
                        "rule with internal id `{rule_id}` could not be found in docs db"
                    );
                    return Ok(None);
                };

                let Some(actions) = rule.field("actions")? else {
                    return Ok(None);
                };
                let actions: Result<Vec<RuleAction>, serde_json::Error> =
                    serde_json::from_str(actions.get());
                match actions {
                    Ok(actions) => Ok(Some(actions.into_iter())),
                    Err(err) => {
                        tracing::warn!(
                        "could not deserialize actions of rule with internal id `{rule_id}`: {err}"
                    );
                        return Ok(None);
                    }
                }
            })
            .filter_map(|x| x.transpose())
            .flatten_ok()
            .filter_map_ok(|action| {
                let Some(doc_id) = action.active_document(search_context).transpose() else {
                    return None;
                };

                let doc_id = match doc_id {
                    Ok(doc_id) => doc_id,
                    Err(err) => return Some(Err(err)),
                };
                match action.action {
                    DynamicSearchRuleAction::Pin { position } => {
                        Some(Ok(PinDoc { pos: position, doc_id }))
                    }
                }
            })
            .map(|x| x.flatten())
    }

    fn active_rules(
        &self,
        query_terms: &[LocatedQueryTerm],
        search_context: &SearchContext,
    ) -> Result<RoaringBitmap> {
        // 1. include rules that are active
        let mut active_rules = if let Some(active_fid) = self.db_fields_ids_map.id("active") {
            let active_key = FacetGroupKey { field_id: active_fid, level: 0, left_bound: "true" };
            let Some(FacetGroupValue { size: _, bitmap: active_rules }) =
                self.index.facet_id_string_docids.get(&self.rtxn, &active_key)?
            else {
                return Ok(RoaringBitmap::new());
            };
            active_rules
        } else {
            self.index.documents_ids(&self.rtxn)?
        };

        // 2. exclude rules that have a time condition that is not met
        let target_time = search_context.before_search.format(&Rfc3339).unwrap();
        let db = self.index.facet_id_string_docids;
        if let Some(time_start_fid) = self.db_fields_ids_map.id("conditions.time.start") {
            let mut time_start_after_now = RoaringBitmap::new();

            // looking for all rules whose time.start is AFTER target_time
            // so ]target_time, ..]
            let left = Bound::Excluded(target_time.as_str());
            let right = Bound::Unbounded;
            find_docids_of_facet_within_bounds(
                &self.rtxn,
                db,
                time_start_fid,
                &left,
                &right,
                Some(&active_rules),
                &mut time_start_after_now,
            )?;
            active_rules -= time_start_after_now;
        }
        if let Some(time_end_fid) = self.db_fields_ids_map.id("conditions.time.end") {
            let mut time_end_before_now = RoaringBitmap::new();

            // looking for all rules whose time.end is BEFORE target_time
            // so ].., target_time]
            let left = Bound::Unbounded;
            let right = Bound::Excluded(target_time.as_str());
            find_docids_of_facet_within_bounds(
                &self.rtxn,
                db,
                time_end_fid,
                &left,
                &right,
                Some(&active_rules),
                &mut time_end_before_now,
            )?;
            active_rules -= time_end_before_now;
        }

        // 3. exclude rules that have the a different query emptiness condition
        let is_query_empty = query_terms.is_empty();
        if let Some(is_query_empty_fid) = self.db_fields_ids_map.id("conditions.query.isEmpty") {
            if is_query_empty {
                let is_query_not_empty_key =
                    FacetGroupKey { field_id: is_query_empty_fid, level: 0, left_bound: "false" };
                if let Some(FacetGroupValue { size: _, bitmap: is_query_not_empty_rules }) =
                    self.index.facet_id_string_docids.get(&self.rtxn, &is_query_not_empty_key)?
                {
                    active_rules -= is_query_not_empty_rules;
                }
            } else {
                let is_query_empty_key =
                    FacetGroupKey { field_id: is_query_empty_fid, level: 0, left_bound: "true" };
                if let Some(FacetGroupValue { size: _, bitmap: is_query_empty_rules }) =
                    self.index.facet_id_string_docids.get(&self.rtxn, &is_query_empty_key)?
                {
                    active_rules -= is_query_empty_rules;
                }
            }
        };

        let words_count = query_terms.len().min(MAX_COUNTED_WORDS) as u8;
        if let Some(query_words_fid) = self.db_fields_ids_map.id("conditions.query.words") {
            let word_count_db = &self.index.field_id_word_count_docids;

            // 4. exclude words with more word constraints than present in the query
            if let Some(words_count_plus_one) = words_count.checked_add(1) {
                for res in word_count_db.range(
                    &self.rtxn,
                    &((query_words_fid, words_count_plus_one)..=(query_words_fid, u8::MAX)),
                )? {
                    let ((_, _constraint_count), more_constraints_than_query_rules) = res?;
                    active_rules -= more_constraints_than_query_rules;
                }
            }

            let mut words_rules = Vec::new();
            for word in query_terms.iter().take(words_count.into()) {
                let Some(word) = word.value.original_single_word(search_context) else {
                    continue;
                };
                let word = search_context.word_interner.get(word).as_str();
                let Some(mut word_rules) =
                    self.index.word_fid_docids.get(&self.rtxn, &(word, query_words_fid))?
                else {
                    continue;
                };

                word_rules ^= &active_rules;

                if word_rules.is_empty() {
                    continue;
                }

                words_rules.push(word_rules);
            }

            wip::fixme!("dont forget rules that don't have a query.words field");

            // will be populated with all the rules that have no constraints on the words,
            // or rules whose constraints on the words are satisfied by the query
            let mut constraint_all_words_rules = RoaringBitmap::new();

            // 5. check that the correct constraints are present
            for constraint_count in 0..=words_rules.len() {
                // no wraparound in the truncation: words_rules.len() < words_count which is capped to a u8
                let constraint_count = constraint_count as u8;
                let Some(constraint_count_rules) =
                    word_count_db.get(&self.rtxn, &(query_words_fid, constraint_count))?
                else {
                    continue;
                };

                match constraint_count {
                    0 => {
                        constraint_all_words_rules |= &constraint_count_rules;
                    }
                    1 => {
                        for word_rules in words_rules.iter() {
                            constraint_all_words_rules |= &constraint_count_rules ^ word_rules;
                        }
                    }
                    k => {
                        for word_rules in words_rules.iter().combinations(k.into()) {
                            constraint_all_words_rules |= roaring::MultiOps::intersection(
                                std::iter::once(&constraint_count_rules)
                                    .chain(word_rules.into_iter()),
                            );
                        }
                    }
                }
            }

            active_rules ^= constraint_all_words_rules;
        }

        Ok(active_rules)
    }

    fn rule_ids_sorted_by_precedence(
        &self,
        active_rules: RoaringBitmap,
    ) -> Result<impl Iterator<Item = Result<RuleId>> + '_> {
        let db = self.index.facet_id_f64_docids.remap_types();

        if let Some(precedence_field_id) = self.db_fields_ids_map.id("precedence") {
            Ok(either::Left(
                ascending_facet_sort(&self.rtxn, db, precedence_field_id, active_rules)?.flat_map(
                    |res| match res {
                        Ok((bucket, _precedence)) => {
                            either::Either::Left(bucket.into_iter().map(Ok))
                        }
                        Err(err) => either::Either::Right(std::iter::once(Err(err.into()))),
                    },
                ),
            ))
        } else {
            Ok(either::Right(active_rules.into_iter().map(Ok)))
        }
    }
}

#[derive(Deserialize, Debug, Clone, PartialEq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct RuleAction {
    /// Target document selector for this action.
    pub selector: Selector,
    // Use Object here because utoipa's tagged-enum schema generation combines
    // allOf with additionalProperties: false in a way that Spectral rejects.
    /// Action payload to apply to the selected document.
    pub action: DynamicSearchRuleAction,
}

impl RuleAction {
    fn active_document(&self, search_context: &SearchContext<'_>) -> Result<Option<DocumentId>> {
        if self.selector.index_uid.as_ref().is_some_and(|selector_index_uid| {
            selector_index_uid.as_str() != search_context.index_uid
        }) {
            return Ok(None);
        }
        wip::fixme!("check current behavior of main when adding a DSR with a pin without a selector.id. add a unit test if necessary");

        let Some(docid) = self.selector.id.as_deref() else { return Ok(None) };

        Ok(search_context.index.external_documents_ids().get(search_context.txn, docid)?)
    }
}

#[derive(Deserialize, Debug, Clone, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct Selector {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub index_uid: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub id: Option<String>,
}

#[derive(Deserialize, Debug, Clone, PartialEq, Eq)]
#[serde(tag = "type", rename_all = "camelCase", deny_unknown_fields)]
enum DynamicSearchRuleAction {
    Pin { position: u32 },
}
