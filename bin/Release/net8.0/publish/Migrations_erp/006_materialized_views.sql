-- Auto-generated tenant migration from golden dump.
-- Runtime placeholders: {schema}, {schema_name}, {user_password}
SET LOCAL search_path = {schema}, pg_catalog;
SET LOCAL check_function_bodies = false;

CREATE MATERIALIZED VIEW {schema}.mv_accountsdetails AS
 SELECT b.company,
    b.sourceid,
    b.mapname AS omapname,
        CASE
            WHEN (b.mapname IS NOT NULL) THEN (regexp_replace((b.mapname)::text, '[0-9]+'::text, ''::text, 'g'::text))::character varying
            ELSE b.transid
        END AS transid,
    r.branchid,
    r.branchname,
    b.transid AS otransid,
    b.accountshdrid,
    v.vouchertypeid,
    v.vouchertype,
    c.accountsdtlid,
    b.docid AS voucher_number,
    b.finyr AS financial_year,
    b.docdate AS voucher_date,
    b.refno AS reference_number,
    b.refdate AS reference_date,
    b.cheque_number,
    b.cheque_date,
    b.supply_type,
    b.currency AS txn_currencyid,
    d.currency AS txn_currency,
    b.exrate AS txn_exrate,
    COALESCE(b.reconciled, 'F'::character varying) AS reconciled,
        CASE
            WHEN ((COALESCE(b.reconciled, 'F'::character varying))::text = 'F'::text) THEN NULL::date
            ELSE b.reconciled_date
        END AS reconciled_date,
    m.mg_accountid,
    m.accountname,
    l.mg_subledgerid,
    l.subledgername,
    c.bdbamount AS base_debit,
    c.bcramount AS base_credit,
    c.dbamount AS txn_debit,
    c.cramount AS txn_credit,
    c.ndbamount AS native_debit,
    c.ncramount AS native_credit,
    e.currencyid AS native_currencyid,
    e.currency AS native_currency,
    c.aexrate AS native_exrate,
    b.remarks,
    c.narration,
    b.modifiedon,
    b.username,
    m.groupname,
    m.alie,
    m.accountcode_alias,
    m.acatcode
   FROM {schema}.accountshdr b,
    {schema}.accountsdtl c,
    {schema}.currency d,
    {schema}.currency e,
    {schema}.mg_subledger l,
    {schema}.branch r,
    {schema}.vouchertype v,
    {schema}.mg_account m
  WHERE ((b.accountshdrid = c.accountshdrid) AND (b.currency = d.currencyid) AND (c.acurrency = e.currencyid) AND (c.subledgername = l.mg_subledgerid) AND ((b.cancel)::text = 'F'::text) AND (b.app_desc = (1)::numeric) AND (b.branch = r.branchid) AND (b.subtypename = v.vouchertypeid) AND (c.accountname = m.mg_accountid))
  ORDER BY b.company, b.docdate
  WITH NO DATA;

CREATE MATERIALIZED VIEW {schema}.mv_stockdetails AS
 SELECT b.location,
    b.branch,
    b.company,
    b.docdate,
    {schema}.func_returns_hyperlink(b.docid, 'formpage'::character varying, (regexp_replace((b.mapname)::text, '[0-9]+'::text, ''::text, 'g'::text))::character varying, COALESCE(NULLIF({schema}.fn_findheaderid((regexp_replace((b.mapname)::text, '[0-9]+'::text, ''::text, 'g'::text))::character varying, b.sourceid), (0)::numeric), b.sourceid), ((t.caption)::text)::character varying, b.docid) AS docid,
    t.caption AS vouchertype,
    b.mapname,
    b.username,
    b.createdon,
    b.modifiedon,
    b.createdby,
    b.plusorminus,
    i.itemid,
    i.itemname,
    r.branchname,
    l.locationname,
    t.caption,
    b.docid AS mdocid,
    b.stock_qty AS issued_qty,
    b.amount AS issued_value,
    0 AS received_qty,
    0 AS received_value,
    b.rate,
    b.qty,
    b.stock_qty,
    b.cancel
   FROM {schema}.item i,
    {schema}.location l,
    {schema}.branch r,
    ( SELECT a_1.name,
            a_1.caption
           FROM {schema}.tstructs a_1
          WHERE (a_1.blobno = (1)::numeric)) t,
    {schema}.stockvalue b
  WHERE ((b.app_desc = (1)::numeric) AND (b.itemname = i.itemid) AND (b.location = l.locationid) AND (b.branch = r.branchid) AND (regexp_replace((b.mapname)::text, '[0-9]+'::text, ''::text, 'g'::text) = (t.name)::text) AND (lower((b.plusorminus)::text) = 'm'::text) AND ((b.batch)::text <> 'NA'::text))
UNION ALL
 SELECT a.location,
    a.branch,
    a.company,
    a.docdate,
    {schema}.func_returns_hyperlink(a.docid, 'formpage'::character varying, (regexp_replace((a.mapname)::text, '[0-9]+'::text, ''::text, 'g'::text))::character varying, COALESCE(NULLIF({schema}.fn_findheaderid((regexp_replace((a.mapname)::text, '[0-9]+'::text, ''::text, 'g'::text))::character varying, a.sourceid), (0)::numeric), a.sourceid), ((t.caption)::text)::character varying, a.docid) AS docid,
    t.caption AS vouchertype,
    a.mapname,
    a.username,
    a.createdon,
    a.modifiedon,
    a.createdby,
    a.plusorminus,
    i.itemid,
    i.itemname,
    r.branchname,
    l.locationname,
    t.caption,
        CASE
            WHEN (lower((a.plusorminus)::text) = 'm'::text) THEN a.docid
            ELSE a.docid
        END AS mdocid,
        CASE
            WHEN (lower((a.plusorminus)::text) = 'm'::text) THEN a.stock_qty
            ELSE (0)::numeric
        END AS issued_qty,
        CASE
            WHEN (lower((a.plusorminus)::text) = 'm'::text) THEN a.amount
            ELSE (0)::numeric
        END AS issued_value,
        CASE
            WHEN ((lower((a.plusorminus)::text) = 'p'::text) AND ((a.batch)::text = 'NA'::text)) THEN a.stock_qty
            ELSE (0)::numeric
        END AS received_qty,
        CASE
            WHEN ((lower((a.plusorminus)::text) = 'p'::text) AND ((a.batch)::text = 'NA'::text)) THEN a.amount
            ELSE (0)::numeric
        END AS received_value,
        CASE
            WHEN ((lower((a.plusorminus)::text) = 'p'::text) AND ((a.batch)::text = 'NA'::text)) THEN a.rate
            ELSE a.rate
        END AS rate,
    a.qty,
    a.stock_qty,
    a.cancel
   FROM {schema}.item i,
    {schema}.location l,
    {schema}.branch r,
    ( SELECT a_1.name,
            a_1.caption
           FROM {schema}.tstructs a_1
          WHERE (a_1.blobno = (1)::numeric)) t,
    {schema}.stockvalue a
  WHERE ((a.app_desc = (1)::numeric) AND (a.itemname = i.itemid) AND (a.location = l.locationid) AND (a.branch = r.branchid) AND (regexp_replace((a.mapname)::text, '[0-9]+'::text, ''::text, 'g'::text) = (t.name)::text) AND ((a.batch)::text = 'NA'::text))
UNION ALL
 SELECT a.location,
    a.branch,
    a.company,
    a.docdate,
    {schema}.func_returns_hyperlink(a.docid, 'formpage'::character varying, (regexp_replace((a.mapname)::text, '[0-9]+'::text, ''::text, 'g'::text))::character varying, COALESCE(NULLIF({schema}.fn_findheaderid((regexp_replace((a.mapname)::text, '[0-9]+'::text, ''::text, 'g'::text))::character varying, a.sourceid), (0)::numeric), a.sourceid), ((t.caption)::text)::character varying, a.docid) AS docid,
    t.caption AS vouchertype,
    a.mapname,
    a.username,
    a.createdon,
    a.modifiedon,
    a.createdby,
    a.plusorminus,
    i.itemid,
    i.itemname,
    r.branchname,
    l.locationname,
    t.caption,
    a.docid AS mdocid,
    (0)::numeric AS issued_qty,
    (0)::numeric AS issued_value,
    a.stock_qty AS received_qty,
    a.amount AS received_value,
    a.rate,
    a.qty,
    a.stock_qty,
    a.cancel
   FROM {schema}.item i,
    {schema}.location l,
    {schema}.branch r,
    ( SELECT a_1.name,
            a_1.caption
           FROM {schema}.tstructs a_1
          WHERE (a_1.blobno = (1)::numeric)) t,
    {schema}.stockvalue a
  WHERE ((a.app_desc = (1)::numeric) AND (a.itemname = i.itemid) AND (a.location = l.locationid) AND (a.branch = r.branchid) AND (regexp_replace((a.mapname)::text, '[0-9]+'::text, ''::text, 'g'::text) = (t.name)::text) AND (((a.batch)::text <> 'NA'::text) OR (a.batch IS NULL) OR (length(TRIM(BOTH FROM a.batch)) = 0)) AND (lower((a.plusorminus)::text) = 'p'::text))
  ORDER BY 3, 15, 16, 17, 6, 10, 13 DESC
  WITH NO DATA;

CREATE MATERIALIZED VIEW {schema}.mv_accountmonthlysummary AS
 SELECT b.company,
    r.branchid,
    r.branchname,
    to_char((b.docdate)::timestamp with time zone, 'YYYY'::text) AS financial_year,
    (to_char((b.docdate)::timestamp with time zone, 'YYYYMM'::text))::numeric AS voucher_yearmonth,
    to_char((b.docdate)::timestamp with time zone, 'Mon YYYY'::text) AS month_year,
    m.mg_accountid,
    m.accountname,
    l.subledgername,
    e.currencyid AS native_currencyid,
    e.currency AS native_currency,
    m.alie,
    sum(c.bdbamount) AS base_debit,
    sum(c.bcramount) AS base_credit,
    sum(c.ndbamount) AS native_debit,
    sum(c.ncramount) AS native_credit,
    m.controlaccount,
    m.acatcode
   FROM {schema}.accountshdr b,
    {schema}.accountsdtl c,
    {schema}.currency e,
    {schema}.mg_subledger l,
    {schema}.branch r,
    {schema}.mg_account m
  WHERE ((b.accountshdrid = c.accountshdrid) AND (c.acurrency = e.currencyid) AND (c.subledgername = l.mg_subledgerid) AND ((b.cancel)::text = 'F'::text) AND (b.app_desc = (1)::numeric) AND (b.branch = r.branchid) AND (c.accountname = m.mg_accountid))
  GROUP BY b.company, r.branchname, (to_char((b.docdate)::timestamp with time zone, 'YYYY'::text)), c.accountname, l.subledgername, e.currencyid, e.currency, (to_char((b.docdate)::timestamp with time zone, 'YYYYMM'::text))::numeric, m.alie, m.controlaccount, m.mg_accountid, m.accountname, r.branchid, (to_char((b.docdate)::timestamp with time zone, 'Mon YYYY'::text))
  ORDER BY b.company, r.branchname, m.accountname, (to_char((b.docdate)::timestamp with time zone, 'YYYYMM'::text))::numeric
  WITH NO DATA;
