-- storage: the history tables and the daily snapshots use the storage policy "tiered"
-- (clickhouse/files/stellar-config.xml): the local disk holds the newest month, the data volume
-- the rest. A part moves to the volume once its newest row is 30 days old; a part that old from
-- the start, as a backfill writes, goes there directly. Nothing expires: these TTLs only move
-- parts. tx_by_hash has no time column, so it moves only when the local disk runs low;
-- contract_creations is small and unpartitioned, and stays local like the state copy.
-- +migration 14 storage
ALTER TABLE ledgers MODIFY SETTING storage_policy = 'tiered';
ALTER TABLE ledgers MODIFY TTL closed_at + INTERVAL 30 DAY TO VOLUME 'cold';
ALTER TABLE transactions MODIFY SETTING storage_policy = 'tiered';
ALTER TABLE transactions MODIFY TTL closed_at + INTERVAL 30 DAY TO VOLUME 'cold';
ALTER TABLE operations MODIFY SETTING storage_policy = 'tiered';
ALTER TABLE operations MODIFY TTL closed_at + INTERVAL 30 DAY TO VOLUME 'cold';
ALTER TABLE events MODIFY SETTING storage_policy = 'tiered';
ALTER TABLE events MODIFY TTL closed_at + INTERVAL 30 DAY TO VOLUME 'cold';
ALTER TABLE token_transfers MODIFY SETTING storage_policy = 'tiered';
ALTER TABLE token_transfers MODIFY TTL closed_at + INTERVAL 30 DAY TO VOLUME 'cold';
ALTER TABLE transfers_by_account MODIFY SETTING storage_policy = 'tiered';
ALTER TABLE transfers_by_account MODIFY TTL closed_at + INTERVAL 30 DAY TO VOLUME 'cold';
ALTER TABLE account_activity MODIFY SETTING storage_policy = 'tiered';
ALTER TABLE account_activity MODIFY TTL closed_at + INTERVAL 30 DAY TO VOLUME 'cold';
ALTER TABLE contract_calls MODIFY SETTING storage_policy = 'tiered';
ALTER TABLE contract_calls MODIFY TTL closed_at + INTERVAL 30 DAY TO VOLUME 'cold';
ALTER TABLE tx_by_hash MODIFY SETTING storage_policy = 'tiered';
ALTER TABLE asset_snapshots MODIFY SETTING storage_policy = 'tiered';
ALTER TABLE asset_snapshots MODIFY TTL date + INTERVAL 30 DAY TO VOLUME 'cold';
ALTER TABLE holder_snapshots MODIFY SETTING storage_policy = 'tiered';
ALTER TABLE holder_snapshots MODIFY TTL date + INTERVAL 30 DAY TO VOLUME 'cold';
