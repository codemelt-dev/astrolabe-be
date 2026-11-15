-- merge: a partition that has had no insert for a day is merged into one part. A lookup by key
-- reads once per part (an account's history, a transaction by hash), and the backfill left
-- several parts in every month, which ClickHouse wouldn't merge by itself for weeks. The month
-- being written is left alone: it always has young parts. ClickHouse runs these merges only
-- while most of its merge pool is free, so the pipeline's own merges come first.
-- +migration 17 merge
ALTER TABLE ledgers MODIFY SETTING min_age_to_force_merge_seconds = 86400, min_age_to_force_merge_on_partition_only = 1;
ALTER TABLE transactions MODIFY SETTING min_age_to_force_merge_seconds = 86400, min_age_to_force_merge_on_partition_only = 1;
ALTER TABLE operations MODIFY SETTING min_age_to_force_merge_seconds = 86400, min_age_to_force_merge_on_partition_only = 1;
ALTER TABLE events MODIFY SETTING min_age_to_force_merge_seconds = 86400, min_age_to_force_merge_on_partition_only = 1;
ALTER TABLE token_transfers MODIFY SETTING min_age_to_force_merge_seconds = 86400, min_age_to_force_merge_on_partition_only = 1;
ALTER TABLE transfers_by_account MODIFY SETTING min_age_to_force_merge_seconds = 86400, min_age_to_force_merge_on_partition_only = 1;
ALTER TABLE account_activity MODIFY SETTING min_age_to_force_merge_seconds = 86400, min_age_to_force_merge_on_partition_only = 1;
ALTER TABLE contract_calls MODIFY SETTING min_age_to_force_merge_seconds = 86400, min_age_to_force_merge_on_partition_only = 1;
ALTER TABLE tx_by_hash MODIFY SETTING min_age_to_force_merge_seconds = 86400, min_age_to_force_merge_on_partition_only = 1;
ALTER TABLE trades MODIFY SETTING min_age_to_force_merge_seconds = 86400, min_age_to_force_merge_on_partition_only = 1;
