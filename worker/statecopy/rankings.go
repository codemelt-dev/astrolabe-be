package statecopy

import (
	"fmt"
	"strings"
	"time"
)

// rankings are the statements that fill assets_new and top_holders_new from
// the copy's _new tables (and, for Soroban-only tokens, from the history):
// what clickhouse/assets.sql describes. ledger and totalCoins are the
// copy's; computedAt stamps the rows.
func rankings(ledger uint32, totalCoins int64, computedAt time.Time) []string {
	r := strings.NewReplacer(
		"{ledger}", fmt.Sprint(ledger),
		"{total_coins}", fmt.Sprint(totalCoins),
		"{computed_at}", fmt.Sprintf("toDateTime(%d, 'UTC')", computedAt.Unix()),
		"{top}", fmt.Sprint(TopHolders),
		"{top_xlm}", fmt.Sprint(TopXLMHolders),
	)
	out := make([]string, len(rankingSQL))
	for i, q := range rankingSQL {
		out[i] = r.Replace(q)
	}
	return out
}

// classicKeys are the classic assets anything in the copy holds.
const classicKeys = `SELECT asset FROM state_trustlines_new
	UNION DISTINCT SELECT asset FROM state_sac_balances_new WHERE asset != 'native'
	UNION DISTINCT SELECT asset FROM state_asset_contracts_new WHERE asset != 'native'
	UNION DISTINCT SELECT asset FROM state_claimable_balances_new WHERE asset != 'native'
	UNION DISTINCT SELECT asset_a FROM state_liquidity_pools_new WHERE asset_a != 'native'
	UNION DISTINCT SELECT asset_b FROM state_liquidity_pools_new WHERE asset_b != 'native'`

// sorobanBalances are the balances of Soroban-only tokens (SEP-41
// contracts without a Stellar Asset Contract), summed from their transfers.
const sorobanBalances = `SELECT contract_id, account, sum(amount) AS balance FROM transfers_by_account
	WHERE asset = '' GROUP BY contract_id, account`

var rankingSQL = []string{
	// Classic assets: what trustlines, contracts, claimable balances and
	// pools hold, with the issuer's flags and home domain.
	`INSERT INTO assets_new
	SELECT k.asset AS asset, 'classic' AS kind, splitByChar(':', k.asset)[1] AS code, splitByChar(':', k.asset)[2] AS issuer,
	       c.contract_id AS contract_id, t.holders + s.holders AS holders, t.trustlines AS trustlines,
	       t.amount + cb.amount + lp.amount + s.amount AS supply, t.amount AS in_accounts, cb.amount AS in_claimable_balances,
	       lp.amount AS in_liquidity_pools, s.amount AS in_contracts, i.flags AS issuer_flags, i.home_domain AS home_domain,
	       true AS complete, {ledger} AS ledger, {computed_at} AS computed_at
	FROM (` + classicKeys + `) AS k
	LEFT JOIN (SELECT asset, countIf(balance > 0) AS holders, count() AS trustlines, sum(toInt128(balance)) AS amount
	           FROM state_trustlines_new GROUP BY asset) AS t USING asset
	LEFT JOIN (SELECT asset, countIf(balance > 0) AS holders, sum(balance) AS amount FROM state_sac_balances_new GROUP BY asset) AS s USING asset
	LEFT JOIN (SELECT asset, any(contract_id) AS contract_id FROM state_asset_contracts_new GROUP BY asset) AS c USING asset
	LEFT JOIN (SELECT asset, sum(toInt128(amount)) AS amount FROM state_claimable_balances_new GROUP BY asset) AS cb USING asset
	LEFT JOIN (SELECT asset, sum(amount) AS amount FROM (
	               SELECT asset_a AS asset, toInt128(reserve_a) AS amount FROM state_liquidity_pools_new
	               UNION ALL SELECT asset_b AS asset, toInt128(reserve_b) AS amount FROM state_liquidity_pools_new)
	           GROUP BY asset) AS lp USING asset
	LEFT JOIN (SELECT account_id, flags, home_domain FROM state_accounts_new
	           WHERE account_id IN (SELECT splitByChar(':', asset)[2] FROM (` + classicKeys + `))) AS i
	       ON i.account_id = splitByChar(':', k.asset)[2]`,

	// XLM: its supply is the ledger's total_coins; accounts hold it in
	// their balances.
	`INSERT INTO assets_new
	SELECT 'native', 'native', 'XLM', '', c.contract_id, a.holders + s.holders, a.accounts, toInt128({total_coins}),
	       a.amount, cb.amount, lp.amount, s.amount, 0, '', true, {ledger}, {computed_at}
	FROM (SELECT countIf(balance > 0) AS holders, count() AS accounts, sum(toInt128(balance)) AS amount FROM state_accounts_new) AS a
	CROSS JOIN (SELECT countIf(balance > 0) AS holders, sum(balance) AS amount FROM state_sac_balances_new WHERE asset = 'native') AS s
	CROSS JOIN (SELECT any(contract_id) AS contract_id FROM state_asset_contracts_new WHERE asset = 'native') AS c
	CROSS JOIN (SELECT sum(toInt128(amount)) AS amount FROM state_claimable_balances_new WHERE asset = 'native') AS cb
	CROSS JOIN (SELECT sumIf(toInt128(reserve_a), asset_a = 'native') + sumIf(toInt128(reserve_b), asset_b = 'native') AS amount
	            FROM state_liquidity_pools_new) AS lp`,

	// Soroban-only tokens: supply and holders from their transfers since
	// go-live, complete for tokens created since.
	`INSERT INTO assets_new
	SELECT b.contract_id, 'soroban', '', '', b.contract_id, countIf(b.balance > 0), 0, sum(b.balance),
	       sumIf(b.balance, startsWith(b.account, 'G')), 0, 0, sumIf(b.balance, startsWith(b.account, 'C')), 0, '',
	       b.contract_id IN (SELECT contract_id FROM contract_creations), {ledger}, {computed_at}
	FROM (` + sorobanBalances + `) AS b
	GROUP BY b.contract_id`,

	// The top {top} holders of every asset but XLM, accounts and contracts
	// alike; ties go to the lower address.
	`INSERT INTO top_holders_new
	SELECT asset, toUInt16(row_number() OVER (PARTITION BY asset ORDER BY balance DESC, holder)), holder, balance, {ledger}
	FROM (
	    SELECT asset, holder, balance FROM (
	        SELECT asset, account_id AS holder, toInt128(balance) AS balance FROM state_trustlines_new WHERE balance > 0
	        UNION ALL SELECT asset, holder, balance FROM state_sac_balances_new WHERE balance > 0 AND asset != 'native'
	        UNION ALL SELECT contract_id AS asset, account AS holder, balance FROM (` + sorobanBalances + `) WHERE balance > 0)
	    ORDER BY asset, balance DESC, holder
	    LIMIT {top} BY asset)`,

	// XLM's top {top_xlm}: account balances and contracts' SAC balances.
	`INSERT INTO top_holders_new
	SELECT 'native', toUInt16(row_number() OVER (ORDER BY balance DESC, holder)), holder, balance, {ledger}
	FROM (
	    SELECT holder, balance FROM (
	        SELECT account_id AS holder, toInt128(balance) AS balance FROM state_accounts_new WHERE balance > 0
	        UNION ALL SELECT holder, balance FROM state_sac_balances_new WHERE asset = 'native' AND balance > 0)
	    ORDER BY balance DESC, holder
	    LIMIT {top_xlm})`,
}
