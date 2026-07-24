-- Auto-generated tenant migration from golden dump.
-- Runtime placeholders: {schema}, {schema_name}, {user_password}
SET LOCAL search_path = {schema}, pg_catalog;
SET LOCAL check_function_bodies = false;

CREATE VIEW {schema}.ax_inbound_status AS
 SELECT a.filename,
    (a.recdon)::date AS recdon,
    (a.recdon)::date AS indate,
    a.transid,
    b.caption AS tstructname,
    a.instatus
   FROM {schema}.inbound a,
    {schema}.tstructs b
  WHERE ((a.transid)::text = (b.name)::text);

CREATE VIEW {schema}.ax_outbound_status AS
 SELECT a.recordid AS filename,
    (a.senton)::date AS senton,
    (a.senton)::date AS outdate,
    a.transid,
    b.caption AS tstructname,
        CASE
            WHEN (a.senton IS NULL) THEN 'Pending'::text
            ELSE 'Sent'::text
        END AS outstatus
   FROM {schema}.outbound a,
    {schema}.tstructs b
  WHERE ((a.transid)::text = (b.name)::text);

CREATE VIEW {schema}.axp_vw_menu AS
 SELECT replace(replace((COALESCE(h.caption, ''::text) || COALESCE(('\'::text || (g.caption)::text), ''::text)), '\\\'::text, '\'::text), '\\'::text, '\'::text) AS menupath,
    g.caption,
    g.name,
    g.ordno,
    g.levelno,
    g.parent,
    g.type,
    g.pagetype,
    replace(replace(((COALESCE(('\'::text || (g.visible)::text), 'F'::text) || COALESCE(('\'::text || h.visible), ''::text)) || '\'::text), '\\\'::text, '\'::text), '\\'::text, '\'::text) AS visible,
    g.websubtype
   FROM ({schema}.axpages g
     LEFT JOIN ( SELECT (COALESCE(f.caption, ''::text) || COALESCE(('\'::text || (e.caption)::text), ''::text)) AS caption,
            e.parent,
            e.name,
            ((COALESCE(('\'::text || f.visible), ''::text) || COALESCE(('\'::text || (e.visible)::text), ''::text)) || '\'::text) AS visible
           FROM ({schema}.axpages e
             LEFT JOIN ( SELECT (((COALESCE(d.caption, ''::character varying))::text || '\'::text) || (COALESCE(c.caption, ''::character varying))::text) AS caption,
                    c.name,
                    ((COALESCE(('\'::text || d.visible), ''::text) || COALESCE(('\'::text || (c.visible)::text), ''::text)) || '\'::text) AS visible
                   FROM ({schema}.axpages c
                     LEFT JOIN ( SELECT a.name,
                            a.parent,
                            a.caption,
                            a.levelno,
                            a.ordno,
                            1 AS levlno,
                            (('\'::text || (a.visible)::text) || '\'::text) AS visible
                           FROM {schema}.axpages a
                          WHERE (a.levelno = (0)::numeric)
                          ORDER BY a.levelno, a.ordno) d ON (((c.parent)::text = (d.name)::text)))
                  WHERE (c.levelno = ANY (ARRAY[(1)::numeric, (0)::numeric]))) f ON (((e.parent)::text = (f.name)::text)))
          WHERE (e.levelno = ANY (ARRAY[(1)::numeric, (0)::numeric, (2)::numeric]))) h ON (((g.parent)::text = (h.name)::text)))
  WHERE (g.levelno <= (3)::numeric)
  ORDER BY g.ordno, g.levelno;

CREATE VIEW {schema}.axp_appsearch_data_new AS
 SELECT 2 AS slno,
    axp_appsearch_data_v2.hltype,
    axp_appsearch_data_v2.structname,
    btrim(replace((axp_appsearch_data_v2.searchtext)::text, 'View'::text, ' '::text)) AS searchtext,
    axp_appsearch_data_v2.params,
    a.oldappurl
   FROM ({schema}.axp_appsearch_data_v2
     JOIN {schema}.axpages a ON (((
        CASE axp_appsearch_data_v2.hltype
            WHEN 'iview'::text THEN (('i'::text || (axp_appsearch_data_v2.structname)::text))::character varying
            WHEN 'tstruct'::text THEN (('t'::text || (axp_appsearch_data_v2.structname)::text))::character varying
            ELSE axp_appsearch_data_v2.structname
        END)::text = (
        CASE
            WHEN ((axp_appsearch_data_v2.hltype)::text = 'Page'::text) THEN a.name
            ELSE a.pagetype
        END)::text)))
  WHERE (lower((axp_appsearch_data_v2.params)::text) !~~ '%current_date%'::text)
UNION ALL
 SELECT 1.9 AS slno,
    a.hltype,
    a.structname,
    btrim(replace((a.searchtext)::text, 'View'::text, ' '::text)) AS searchtext,
    a.params,
    p.oldappurl
   FROM ({schema}.axp_appsearch_data a
     JOIN {schema}.axpages p ON (((
        CASE a.hltype
            WHEN 'iview'::text THEN (('i'::text || (a.structname)::text))::character varying
            WHEN 'tstruct'::text THEN (('t'::text || (a.structname)::text))::character varying
            ELSE a.structname
        END)::text = (
        CASE
            WHEN ((a.hltype)::text = 'Page'::text) THEN p.name
            ELSE p.pagetype
        END)::text)))
  WHERE (NOT (EXISTS ( SELECT 'x'::text AS text
           FROM {schema}.axp_appsearch_data_v2 b
          WHERE (((a.structname)::text = (b.structname)::text) AND ((a.params)::text = (b.params)::text)))))
UNION ALL
 SELECT 2 AS slno,
    axp_appsearch_data_v2.hltype,
    axp_appsearch_data_v2.structname,
    btrim(replace((axp_appsearch_data_v2.searchtext)::text, 'View'::text, ' '::text)) AS searchtext,
    replace(replace(replace(replace(replace(replace(replace(replace(replace(replace((axp_appsearch_data_v2.params)::text, 'date_trunc(''month'',current_date)'::text, btrim(to_char(date_trunc('month'::text, (CURRENT_DATE)::timestamp with time zone), 'dd/mm/yyyy'::text))), 'date_trunc(''month'',(add_months(current_date,0-1)))'::text, btrim(to_char(date_trunc('month'::text, ({schema}.add_months(CURRENT_DATE, (0 - 1)))::timestamp with time zone), 'dd/mm/yyyy'::text))), 'date_trunc(''month'', current_date) + interval ''0 month'' - interval ''1 day'''::text, btrim(to_char(((date_trunc('month'::text, (CURRENT_DATE)::timestamp with time zone) + '00:00:00'::interval) - '1 day'::interval), 'dd/mm/yyyy'::text))), 'date_trunc(''week'',current_date)'::text, btrim(to_char(date_trunc('week'::text, (CURRENT_DATE)::timestamp with time zone), 'dd/mm/yyyy'::text))), 'date_trunc(''week'',current_date-7)+ interval ''6 day'''::text, btrim(to_char((date_trunc('week'::text, ((CURRENT_DATE - 7))::timestamp with time zone) + '6 days'::interval), 'dd/mm/yyyy'::text))), 'date_trunc(''week'',current_date-7)'::text, btrim(to_char(date_trunc('week'::text, ((CURRENT_DATE - 7))::timestamp with time zone), 'dd/mm/yyyy'::text))), 'date_trunc(''month'',current_date)'::text, btrim(to_char(date_trunc('month'::text, (CURRENT_DATE)::timestamp with time zone), 'dd/mm/yyyy'::text))), 'current_date-1'::text, btrim(to_char(((CURRENT_DATE - 1))::timestamp with time zone, 'dd/mm/yyyy'::text))), 'current_date'::text, btrim(to_char((CURRENT_DATE)::timestamp with time zone, 'dd/mm/yyyy'::text))), ' &'::text, '&'::text) AS params,
    a.oldappurl
   FROM ({schema}.axp_appsearch_data_v2
     JOIN {schema}.axpages a ON (((
        CASE axp_appsearch_data_v2.hltype
            WHEN 'iview'::text THEN (('i'::text || (axp_appsearch_data_v2.structname)::text))::character varying
            WHEN 'tstruct'::text THEN (('t'::text || (axp_appsearch_data_v2.structname)::text))::character varying
            ELSE axp_appsearch_data_v2.structname
        END)::text = (
        CASE
            WHEN ((axp_appsearch_data_v2.hltype)::text = 'Page'::text) THEN a.name
            ELSE a.pagetype
        END)::text)))
  WHERE (lower((axp_appsearch_data_v2.params)::text) ~~ '%current_date%'::text)
UNION ALL
 SELECT 1 AS slno,
    'tstruct'::character varying AS hltype,
    t.name AS structname,
    t.caption AS searchtext,
    NULL::character varying AS params,
    p.oldappurl
   FROM ({schema}.tstructs t
     JOIN {schema}.axpages p ON ((('t'::text || (t.name)::text) = (p.pagetype)::text)))
  WHERE ((t.blobno = (1)::numeric) AND (EXISTS ( SELECT 'x'::text AS text
           FROM {schema}.axp_vw_menu x
          WHERE (((x.pagetype)::text ~~ 't%'::text) AND (btrim(substr((x.pagetype)::text, 2, 20)) = (t.name)::text) AND (x.visible !~~ '%F%'::text)))))
UNION ALL
 SELECT 0 AS slno,
    'iview'::character varying AS hltype,
    i.name AS structname,
    i.caption AS searchtext,
    NULL::character varying AS params,
    p.oldappurl
   FROM ({schema}.iviews i
     JOIN {schema}.axpages p ON ((('i'::text || (i.name)::text) = (p.pagetype)::text)))
  WHERE ((i.blobno = (1)::numeric) AND (EXISTS ( SELECT 'x'::text AS text
           FROM {schema}.axp_vw_menu x
          WHERE (((x.pagetype)::text ~~ 'i%'::text) AND (btrim(substr((x.pagetype)::text, 2, 20)) = (i.name)::text) AND (x.visible !~~ '%F%'::text)))))
UNION ALL
 SELECT 3 AS slno,
    'Page'::character varying AS hltype,
    axp_vw_menu.name AS structname,
    axp_vw_menu.caption AS searchtext,
    NULL::character varying AS params,
    p.oldappurl
   FROM ({schema}.axp_vw_menu
     JOIN {schema}.axpages p ON (((axp_vw_menu.name)::text = (p.name)::text)))
  WHERE ((axp_vw_menu.pagetype)::text = 'web'::text)
  ORDER BY 1;

CREATE VIEW {schema}.axp_appsearch AS
 SELECT DISTINCT searchtext,
    ((params)::text ||
        CASE
            WHEN ((params IS NOT NULL) AND (lower((params)::text) !~~ '%~act%'::text)) THEN '~act=load'::text
            ELSE NULL::text
        END) AS params,
    hltype,
    structname,
    username,
    oldappurl
   FROM ( SELECT s.slno,
            s.searchtext,
            s.params,
            s.hltype,
            s.structname,
            lg.username,
            s.oldappurl
           FROM {schema}.axp_appsearch_data_new s,
            {schema}.axuseraccess a_1,
            {schema}.axusergroups g,
            {schema}.axusergroupsdetail g1,
            {schema}.axuserlevelgroups lg
          WHERE (((a_1.sname)::text = (s.structname)::text) AND (g.axusergroupsid = g1.axusergroupsid) AND ((g.groupname)::text = (lg.usergroup)::text) AND ((a_1.rname)::text = (g1.roles_id)::text) AND ((s.structname)::text <> 'axurg'::text) AND ((a_1.stype)::text = ANY (ARRAY[('t'::character varying)::text, ('i'::character varying)::text])))
          GROUP BY s.searchtext, s.params, s.hltype, s.structname, lg.username, s.slno, s.oldappurl
        UNION
         SELECT b.slno,
            b.searchtext,
            b.params,
            b.hltype,
            b.structname,
            lg.username,
            b.oldappurl
           FROM {schema}.axuserlevelgroups lg,
            ( SELECT DISTINCT s.searchtext,
                    s.params,
                    s.hltype,
                    s.structname,
                    0 AS slno,
                    s.oldappurl
                   FROM ({schema}.axp_appsearch_data_new s
                     LEFT JOIN {schema}.axuseraccess a_1 ON ((((s.structname)::text = (a_1.sname)::text) AND ((a_1.stype)::text = ANY (ARRAY[('t'::character varying)::text, ('i'::character varying)::text])))))) b
          WHERE (((lg.usergroup)::text = 'default'::text) AND ((b.structname)::text <> 'axurg'::text))
  ORDER BY 1, 6) a;

CREATE VIEW {schema}.axp_vw_menulist AS
 SELECT replace(replace((COALESCE(('\'::text || h.caption), ''::text) || COALESCE(('\'::text || (g.caption)::text), ''::text)), '\\\'::text, '\'::text), '\\'::text, '\'::text) AS menupath,
    g.name,
    g.ordno,
    g.levelno,
    g.parent,
    g.type,
    g.pagetype
   FROM ({schema}.axpages g
     LEFT JOIN ( SELECT (COALESCE(('\'::text || f.caption), ''::text) || COALESCE(('\'::text || (e.caption)::text), ''::text)) AS caption,
            e.parent,
            e.name
           FROM ({schema}.axpages e
             LEFT JOIN ( SELECT ((COALESCE(('\'::text || (d.caption)::text), ''::text) || '\'::text) || (COALESCE(c.caption, ''::character varying))::text) AS caption,
                    c.name
                   FROM ({schema}.axpages c
                     LEFT JOIN ( SELECT a.name,
                            a.parent,
                            a.caption,
                            a.levelno,
                            a.ordno,
                            1 AS levlno
                           FROM {schema}.axpages a
                          WHERE (a.levelno = (0)::numeric)
                          ORDER BY a.levelno, a.ordno) d ON (((c.parent)::text = (d.name)::text)))
                  WHERE (c.levelno = ANY (ARRAY[(1)::numeric, (0)::numeric]))) f ON (((e.parent)::text = (f.name)::text)))
          WHERE (e.levelno = ANY (ARRAY[(1)::numeric, (0)::numeric, (2)::numeric]))) h ON (((g.parent)::text = (h.name)::text)))
  WHERE (COALESCE(g.levelno, (0)::numeric) <= (3)::numeric)
  ORDER BY g.ordno, g.levelno;

CREATE VIEW {schema}.vw_account_tree AS
 WITH RECURSIVE comp AS (
         SELECT mg_account.mg_accountid,
            mg_account.accountname,
            mg_account.accountcode,
            mg_account.groupname,
            mg_account.alie,
            mg_account.atype,
            mg_account.primarygroup,
            (mg_account.alie)::character varying(20) AS aprefix,
            0 AS depth,
            ((((mg_account.accountcode)::text || '-'::text) || mg_account.mg_accountid) || ''::text) AS path,
            ((mg_account.accountname)::text || ''::text) AS cpath,
            COALESCE(mg_account.glevel, (0)::numeric) AS grplevel,
            mg_account.addable,
            mg_account.company,
            mg_account.company_code,
            mg_account.acategory
           FROM {schema}.mg_account
          WHERE ((mg_account.groupname IS NULL) AND (mg_account.company = ('1368330000000'::bigint)::numeric))
        UNION
         SELECT c1.mg_accountid,
            c1.accountname,
            c1.accountcode,
            c1.groupname,
            c1.alie,
            c1.atype,
            c1.primarygroup,
            (c1.alie)::character varying(20) AS aprefix,
            (c2.depth + 1) AS depth,
            ((((c2.path || ','::text) || (c1.accountcode)::text) || '-'::text) || c1.mg_accountid) AS path,
            ((c2.cpath || '~'::text) || (c1.accountname)::text) AS cpath,
            COALESCE(c1.glevel, (0)::numeric) AS glevel,
            c1.addable,
            c1.company,
            c1.company_code,
            c1.acategory
           FROM {schema}.mg_account c1,
            comp c2
          WHERE (((c1.groupname)::text = (c2.accountname)::text) AND (c1.company = ('1368330000000'::bigint)::numeric))
        )
 SELECT row_number() OVER (ORDER BY path) AS sno,
    path,
    mg_accountid,
    accountname,
    accountcode,
        CASE
            WHEN (grplevel > (0)::numeric) THEN ((repeat('.'::text, (((grplevel * (5)::numeric) - (1)::numeric))::integer) || (accountname)::text))::character varying
            ELSE accountname
        END AS account,
    groupname,
    alie,
    'None'::text AS subledgername,
    atype,
    primarygroup,
    NULL::text AS collapse,
    grplevel,
    aprefix,
    cpath,
    addable,
    company,
    company_code,
    acategory
   FROM comp a
  ORDER BY alie, path, depth;

CREATE VIEW {schema}.vw_accountsdetails AS
 SELECT a.mg_accountid,
    c.companyid,
    c.companyname,
    c.company_code,
    a.alie,
    a.controlaccount,
    a.acategory,
    a.atype,
    a.groupname,
    a.accountcode,
    a.group_accountcodes,
    b.company,
    b.branchid,
    b.branchname,
    b.transid,
    b.accountshdrid,
    b.vouchertypeid,
    b.vouchertype,
    b.accountsdtlid,
    b.voucher_number,
    b.financial_year,
    b.voucher_date,
    b.txn_currencyid,
    b.txn_currency,
    b.txn_exrate,
    b.accountname,
    b.base_debit,
    b.base_credit,
    b.txn_debit,
    b.txn_credit,
    b.native_debit,
    b.native_credit,
    b.native_currencyid,
    b.native_currency,
    b.native_exrate
   FROM (({schema}.mg_account a
     JOIN ( SELECT b_1.company,
            r.branchid,
            r.branchname,
            b_1.transid,
            b_1.accountshdrid,
            v.vouchertypeid,
            v.vouchertype,
            c_1.accountsdtlid,
            b_1.docid AS voucher_number,
            '2023'::character varying(4) AS financial_year,
            b_1.docdate AS voucher_date,
            b_1.currency AS txn_currencyid,
            d.currency AS txn_currency,
            b_1.exrate AS txn_exrate,
            c_1.accountname,
            c_1.bdbamount AS base_debit,
            c_1.bcramount AS base_credit,
            c_1.dbamount AS txn_debit,
            c_1.cramount AS txn_credit,
            c_1.ndbamount AS native_debit,
            c_1.ncramount AS native_credit,
            e.currencyid AS native_currencyid,
            e.currency AS native_currency,
            c_1.aexrate AS native_exrate
           FROM {schema}.accountshdr b_1,
            {schema}.accountsdtl c_1,
            {schema}.currency d,
            {schema}.currency e,
            {schema}.mg_subledger l,
            {schema}.branch r,
            {schema}.vouchertype v
          WHERE ((b_1.accountshdrid = c_1.accountshdrid) AND (b_1.currency = d.currencyid) AND (c_1.acurrency = e.currencyid) AND (c_1.subledgername = l.mg_subledgerid) AND ((b_1.cancel)::text = 'F'::text) AND (b_1.app_desc = (1)::numeric) AND (b_1.branch = r.branchid) AND (b_1.subtypename = v.vouchertypeid))) b ON (((a.company = b.company) AND (a.mg_accountid = b.accountname))))
     JOIN {schema}.company c ON ((a.company = c.companyid)))
  ORDER BY c.company_code, a.alie, a.group_accountcodes, b.financial_year, b.voucher_date;

CREATE VIEW {schema}.vw_parantlink AS
 SELECT m.company,
    (m.mg_accountid)::numeric AS accountname,
    (m1.mg_accountid)::numeric AS parant1,
    (m2.mg_accountid)::numeric AS parant2,
    (m3.mg_accountid)::numeric AS parant3,
    (m4.mg_accountid)::numeric AS parant4,
    (m5.mg_accountid)::numeric AS parant5,
    (m6.mg_accountid)::numeric AS parant6,
    m.accountname AS caccountname,
    m1.accountname AS cparant1,
    m2.accountname AS cparant2,
    m3.accountname AS cparant3,
    m4.accountname AS cparant4,
    m5.accountname AS cparant5,
    m6.accountname AS cparant6,
    m.common_intergration_code,
    m1.common_intergration_code AS common_intergration_code1,
    m2.common_intergration_code AS common_intergration_code2
   FROM (((((({schema}.mg_account m
     JOIN {schema}.mg_account m1 ON ((((m.groupname)::text = (m1.accountname)::text) AND ((m.atype)::text = 'Account'::text) AND (m.company = m1.company))))
     LEFT JOIN {schema}.mg_account m2 ON ((((m1.groupname)::text = (m2.accountname)::text) AND (m1.company = m2.company))))
     LEFT JOIN {schema}.mg_account m3 ON ((((m2.groupname)::text = (m3.accountname)::text) AND (m2.company = m3.company))))
     LEFT JOIN {schema}.mg_account m4 ON ((((m3.groupname)::text = (m4.accountname)::text) AND (m3.company = m4.company))))
     LEFT JOIN {schema}.mg_account m5 ON ((((m4.groupname)::text = (m5.accountname)::text) AND (m4.company = m5.company))))
     LEFT JOIN {schema}.mg_account m6 ON ((((m5.groupname)::text = (m6.accountname)::text) AND (m5.company = m6.company))))
  ORDER BY m.alie, m.company, m1.accountname, m2.accountname, m3.accountname, m4.accountname, m5.accountname, m.accountname;

CREATE VIEW {schema}.vw_accounttreelink AS
 SELECT
        CASE lvl
            WHEN 0 THEN accountname
            WHEN 1 THEN parant1
            WHEN 2 THEN parant2
            WHEN 3 THEN parant3
            WHEN 4 THEN parant4
            WHEN 5 THEN parant5
            ELSE parant6
        END AS parantid,
    accountname AS childid,
    lvl AS child_level
   FROM ( SELECT a_1.accountname,
            a_1.parant1,
            a_1.parant2,
            a_1.parant3,
            a_1.parant4,
            a_1.parant5,
            a_1.parant6,
            b.lvl
           FROM {schema}.vw_parantlink a_1,
            ( SELECT (b_1.lvl - 1) AS lvl
                   FROM ( SELECT row_number() OVER (ORDER BY tstructs.name) AS lvl
                           FROM {schema}.tstructs) b_1
                  WHERE (b_1.lvl <= 7)) b) a
  WHERE (
        CASE lvl
            WHEN 0 THEN accountname
            WHEN 1 THEN parant1
            WHEN 2 THEN parant2
            WHEN 3 THEN parant3
            WHEN 4 THEN parant4
            WHEN 5 THEN parant5
            ELSE parant6
        END IS NOT NULL)
  ORDER BY accountname,
        CASE lvl
            WHEN 0 THEN accountname
            WHEN 1 THEN parant1
            WHEN 2 THEN parant2
            WHEN 3 THEN parant3
            WHEN 4 THEN parant4
            WHEN 5 THEN parant5
            ELSE parant6
        END;

CREATE VIEW {schema}.vw_arapoutstanding AS
 SELECT a.company,
    a.branch,
    a.subledgercode,
    a.arapdetailsid,
    a.docid,
    a.docdate,
    a.refno,
    a.refdate,
    a.currency,
    a.exrate,
    a.amount,
    a.settled,
    c.currency AS txncurrency
   FROM {schema}.arapdetails a,
    {schema}.currency c
  WHERE (((a.cancel)::text = 'F'::text) AND (a.currency = c.currencyid))
  ORDER BY a.company, a.subledgercode, a.docid, a.docdate, a.branch;

CREATE VIEW {schema}.vw_axlanguage_export AS
 SELECT 'tstruct'::text AS comptype,
    0 AS ord,
    ('t'::text || (tstructs.name)::text) AS ntransid,
    'x__headtext'::character varying AS compname,
    tstructs.caption,
    0 AS ord2,
    0 AS ord3,
    'NA'::text AS hidden
   FROM {schema}.tstructs
UNION ALL
 SELECT 'tstruct'::text AS comptype,
    1 AS ord,
    ('t'::text || (axpdc.tstruct)::text) AS ntransid,
    axpdc.dname AS compname,
    axpdc.caption,
    ("substring"((axpdc.dname)::text, 3))::numeric AS ord2,
    ("substring"((axpdc.dname)::text, 3))::numeric AS ord3,
    'NA'::text AS hidden
   FROM {schema}.axpdc
UNION ALL
 SELECT 'tstruct'::text AS comptype,
    2 AS ord,
    ('t'::text || (axpflds.tstruct)::text) AS ntransid,
    axpflds.fname AS compname,
    axpflds.caption,
    ("substring"((axpflds.dcname)::text, 3))::numeric AS ord2,
    axpflds.ordno AS ord3,
        CASE
            WHEN ((axpflds.hidden)::text = 'TRUE'::text) THEN 'Yes'::text
            ELSE 'No'::text
        END AS hidden
   FROM {schema}.axpflds
UNION ALL
 SELECT 'tstruct'::text AS comptype,
    4 AS ord,
    ('t'::text || (axtoolbar.name)::text) AS ntransid,
    axtoolbar.key AS compname,
    axtoolbar.title AS caption,
    '100'::numeric AS ord2,
    axtoolbar.ordno AS ord3,
    'NA'::text AS hidden
   FROM {schema}.axtoolbar
  WHERE ((axtoolbar.stype)::text = 'tstruct'::text)
UNION ALL
 SELECT 'tstruct'::text AS comptype,
    5 AS ord,
    ('t'::text || (b.name)::text) AS ntransid,
    a.ctype AS compname,
    a.ccaption AS caption,
    a.ord AS ord2,
    0 AS ord3,
    'NA'::text AS hidden
   FROM ( SELECT 'pop1'::text AS ctype,
            'Remove'::text AS ccaption,
            10001 AS ord
        UNION ALL
         SELECT 'pop2'::text AS text,
            'Print'::text AS text,
            10002
        UNION ALL
         SELECT 'pop3'::text AS text,
            'Preview'::text AS text,
            10003
        UNION ALL
         SELECT 'pop4'::text AS text,
            'Regenerate Packets'::text AS text,
            10004
        UNION ALL
         SELECT 'pop5'::text AS text,
            'View History'::text AS text,
            10005
        UNION ALL
         SELECT 'lpop1'::text AS text,
            'Remove'::text AS text,
            10006
        UNION ALL
         SELECT 'lpop2'::text AS text,
            'Print'::text AS text,
            10007
        UNION ALL
         SELECT 'lpop3'::text AS text,
            'Preview'::text AS text,
            10008
        UNION ALL
         SELECT 'lpop4'::text AS text,
            'Params'::text AS text,
            10009
        UNION ALL
         SELECT 'lpop5'::text AS text,
            'Preview Form'::text AS text,
            10010
        UNION ALL
         SELECT 'lpop6'::text AS text,
            'Print Form'::text AS text,
            10011
        UNION ALL
         SELECT 'lpop7'::text AS text,
            'PDF'::text AS text,
            10012
        UNION ALL
         SELECT 'lpop8'::text AS text,
            'Regenerate Packets'::text AS text,
            10013
        UNION ALL
         SELECT 'lpop9'::text AS text,
            'Save As'::text AS text,
            10014
        UNION ALL
         SELECT 'lpop10'::text AS text,
            'To XL'::text AS text,
            10015
        UNION ALL
         SELECT 'lpop11'::text AS text,
            'Rapid XL Export'::text AS text,
            10016
        UNION ALL
         SELECT 'lpop12'::text AS text,
            'View Attachment'::text AS text,
            10017
        UNION ALL
         SELECT 'lblSearh'::text AS text,
            'Search For'::text AS text,
            10018
        UNION ALL
         SELECT 'lblWith'::text AS text,
            'With'::text AS text,
            10019) a,
    {schema}.tstructs b
UNION ALL
 SELECT 'AxPages'::text AS comptype,
    axpages.levelno AS ord,
    NULL::character varying AS ntransid,
    axpages.name AS compname,
    axpages.caption,
    axpages.ordno AS ord2,
    0 AS ord3,
    'NA'::text AS hidden
   FROM {schema}.axpages
UNION ALL
 SELECT 'iview'::text AS comptype,
    0 AS ord,
    ('i'::text || (iviews.name)::text) AS ntransid,
    'x__head'::character varying AS compname,
    iviews.caption,
    1 AS ord2,
    0 AS ord3,
    'NA'::text AS hidden
   FROM {schema}.iviews
UNION ALL
 SELECT 'iview'::text AS comptype,
    1 AS ord,
    ('i'::text || (iviewmain.iname)::text) AS ntransid,
    'RH1'::character varying AS compname,
    iviewmain.header1 AS caption,
    2 AS ord2,
    0 AS ord3,
    'NA'::text AS hidden
   FROM {schema}.iviewmain
UNION ALL
 SELECT 'iview'::text AS comptype,
    2 AS ord,
    ('i'::text || (iviewparams.iname)::text) AS ntransid,
    iviewparams.pname AS compname,
    iviewparams.pcaption AS caption,
    iviewparams.ordno AS ord2,
    0 AS ord3,
    'NA'::text AS hidden
   FROM {schema}.iviewparams
UNION ALL
 SELECT 'iview'::text AS comptype,
    3 AS ord,
    ('i'::text || (iviewcols.iname)::text) AS ntransid,
    iviewcols.f_name AS compname,
    iviewcols.f_caption AS caption,
    iviewcols.ordno AS ord2,
    0 AS ord3,
    'NA'::text AS hidden
   FROM {schema}.iviewcols
UNION ALL
 SELECT 'iview'::text AS comptype,
    4 AS ord,
    ('i'::text || (axtoolbar.name)::text) AS ntransid,
    axtoolbar.key AS compname,
    axtoolbar.title AS caption,
    axtoolbar.ordno AS ord2,
    0 AS ord3,
    'NA'::text AS hidden
   FROM {schema}.axtoolbar
  WHERE ((axtoolbar.stype)::text = 'iview'::text)
UNION ALL
 SELECT 'iview'::text AS comptype,
    5 AS ord,
    ('i'::text || (b.name)::text) AS ntransid,
    a.ctype AS compname,
    a.ccaption AS caption,
    a.ord AS ord2,
    0 AS ord3,
    'NA'::text AS hidden
   FROM {schema}.iviews b,
    ( SELECT 'anac1'::text AS ctype,
            'Column Heading'::text AS ccaption,
            1 AS ord
        UNION ALL
         SELECT 'anac2'::text AS text,
            'Operator'::text AS text,
            2
        UNION ALL
         SELECT 'anac3'::text AS text,
            'Value (s)'::text AS text,
            3
        UNION ALL
         SELECT 'anac4'::text AS text,
            'Relations'::text AS text,
            4
        UNION ALL
         SELECT 'pop1'::text AS text,
            'Delete'::text AS text,
            5
        UNION ALL
         SELECT 'pop2'::text AS text,
            'New'::text AS text,
            6
        UNION ALL
         SELECT 'pop3'::text AS text,
            'Params'::text AS text,
            7
        UNION ALL
         SELECT 'pop4'::text AS text,
            'Preview Form'::text AS text,
            8
        UNION ALL
         SELECT 'pop5'::text AS text,
            'Print Form'::text AS text,
            9
        UNION ALL
         SELECT 'pop6'::text AS text,
            'PDF'::text AS text,
            10
        UNION ALL
         SELECT 'pop7'::text AS text,
            'Regenerate Packets'::text AS text,
            11
        UNION ALL
         SELECT 'pop8'::text AS text,
            'Save As'::text AS text,
            12
        UNION ALL
         SELECT 'pop9'::text AS text,
            'To XL'::text AS text,
            13
        UNION ALL
         SELECT 'pop10'::text AS text,
            'Rapid XL Export'::text AS text,
            14
        UNION ALL
         SELECT 'pop11'::text AS text,
            'View Attachment'::text AS text,
            15) a;

CREATE VIEW {schema}.vw_bsheetsetup AS
(
        (
                 SELECT 10 AS id,
                    b.accountname AS category,
                    b.mg_accountid AS groupid,
                    b.accountname AS groupname,
                    (COALESCE(a.mg_accountid, b.mg_accountid))::numeric AS mg_accountid,
                    a.accountname,
                    a.accountcode_alias,
                    a.atype,
                    b.alie,
                    b.common_intergration_code,
                    b.company,
                    upper((b.primarygroup)::text) AS primarygroup
                   FROM ({schema}.mg_account a
                     RIGHT JOIN ( SELECT b_1.accountname,
                            b_1.mg_accountid,
                            b_1.alie,
                            b_1.common_intergration_code,
                            b_1.company,
                            b_1.primarygroup
                           FROM {schema}.mg_account b_1
                          WHERE (b_1.common_intergration_code = ('1355770000000'::bigint)::numeric)) b ON (((a.groupname)::text = (b.accountname)::text)))
                UNION
                 SELECT 20 AS id,
                    b.accountname AS category,
                    b.mg_accountid AS groupid,
                    b.accountname AS groupname,
                    (COALESCE(a.mg_accountid, b.mg_accountid))::numeric AS mg_accountid,
                    a.accountname,
                    a.accountcode_alias,
                    a.atype,
                    b.alie,
                    b.common_intergration_code,
                    b.company,
                    upper((b.primarygroup)::text) AS primarygroup
                   FROM ({schema}.mg_account a
                     RIGHT JOIN ( SELECT b_1.accountname,
                            b_1.mg_accountid,
                            b_1.alie,
                            b_1.common_intergration_code,
                            b_1.company,
                            b_1.primarygroup
                           FROM {schema}.mg_account b_1
                          WHERE (b_1.common_intergration_code = ('1359330000000'::bigint)::numeric)) b ON (((a.groupname)::text = (b.accountname)::text)))
                UNION
                 SELECT 40 AS id,
                    'WORKING CAPITAL'::character varying AS category,
                    b.mg_accountid AS groupid,
                    b.accountname AS groupname,
                    (COALESCE(a.mg_accountid, b.mg_accountid))::numeric AS mg_accountid,
                    a.accountname,
                    a.accountcode_alias,
                    a.atype,
                    b.alie,
                    b.common_intergration_code,
                    b.company,
                    upper((b.primarygroup)::text) AS primarygroup
                   FROM ({schema}.mg_account a
                     RIGHT JOIN ( SELECT b_1.accountname,
                            b_1.mg_accountid,
                            b_1.alie,
                            b_1.common_intergration_code,
                            b_1.company,
                            b_1.primarygroup
                           FROM {schema}.mg_account b_1
                          WHERE (b_1.common_intergration_code = ('1355880000000'::bigint)::numeric)) b ON (((a.groupname)::text = (b.accountname)::text)))
                UNION
                 SELECT 50 AS id,
                    'WORKING CAPITAL'::character varying AS category,
                    b.mg_accountid AS groupid,
                    ('Less: '::text || (b.accountname)::text) AS groupname,
                    (COALESCE(a.mg_accountid, b.mg_accountid))::numeric AS mg_accountid,
                    a.accountname,
                    a.accountcode_alias,
                    a.atype,
                    b.alie,
                    b.common_intergration_code,
                    b.company,
                    'ASSETS'::text AS primarygroup
                   FROM ({schema}.mg_account a
                     RIGHT JOIN ( SELECT b_1.accountname,
                            b_1.mg_accountid,
                            b_1.alie,
                            b_1.common_intergration_code,
                            b_1.company,
                            b_1.primarygroup
                           FROM {schema}.mg_account b_1
                          WHERE (b_1.common_intergration_code = ('1358990000000'::bigint)::numeric)) b ON (((a.groupname)::text = (b.accountname)::text)))
        ) UNION ALL
         SELECT 60 AS id,
            'ASSETS'::character varying AS category,
            b.mg_accountid AS groupid,
            ('Less: '::text || (b.accountname)::text) AS groupname,
            (COALESCE(a.mg_accountid, b.mg_accountid))::numeric AS mg_accountid,
            a.accountname,
            a.accountcode_alias,
            a.atype,
            b.alie,
            b.common_intergration_code,
            b.company,
            'ASSETS'::text AS primarygroup
           FROM ({schema}.mg_account a
             RIGHT JOIN ( SELECT b_1.accountname,
                    b_1.mg_accountid,
                    b_1.alie,
                    b_1.common_intergration_code,
                    b_1.company,
                    b_1.primarygroup
                   FROM {schema}.mg_account b_1
                  WHERE (b_1.common_intergration_code = ANY (ARRAY[('1355770000000'::bigint)::numeric, ('1359330000000'::bigint)::numeric, ('1355880000000'::bigint)::numeric, ('1358990000000'::bigint)::numeric]))) b ON (((a.groupname)::text = (b.accountname)::text)))
        UNION ALL
         SELECT 70 AS id,
            'LIABILITIES '::character varying AS category,
            b.mg_accountid AS groupid,
            b.accountname AS groupname,
            (COALESCE(a.mg_accountid, b.mg_accountid))::numeric AS mg_accountid,
            a.accountname,
            a.accountcode_alias,
            a.atype,
            b.alie,
            b.common_intergration_code,
            b.company,
            upper((b.primarygroup)::text) AS primarygroup
           FROM ({schema}.mg_account a
             RIGHT JOIN ( SELECT b_1.accountname,
                    b_1.mg_accountid,
                    b_1.alie,
                    b_1.common_intergration_code,
                    b_1.company,
                    b_1.primarygroup
                   FROM {schema}.mg_account b_1
                  WHERE (b_1.common_intergration_code = ('1358880000000'::bigint)::numeric)) b ON (((a.groupname)::text = (b.accountname)::text)))
) UNION
 SELECT 90 AS id,
    'LIABILITIES '::character varying AS category,
    b.mg_accountid AS groupid,
    b.accountname AS groupname,
    (COALESCE(a.mg_accountid, b.mg_accountid))::numeric AS mg_accountid,
    a.accountname,
    a.accountcode_alias,
    a.atype,
    b.alie,
    b.common_intergration_code,
    b.company,
    upper((b.primarygroup)::text) AS primarygroup
   FROM ({schema}.mg_account a
     RIGHT JOIN ( SELECT b_1.accountname,
            b_1.mg_accountid,
            b_1.alie,
            b_1.common_intergration_code,
            b_1.company,
            b_1.primarygroup
           FROM {schema}.mg_account b_1
          WHERE (b_1.common_intergration_code = ('1359550000000'::bigint)::numeric)) b ON (((a.groupname)::text = (b.accountname)::text)))
UNION
 SELECT 100 AS id,
    'LIABILITIES '::character varying AS category,
    b.mg_accountid AS groupid,
    b.accountname AS groupname,
    (COALESCE(a.mg_accountid, b.mg_accountid))::numeric AS mg_accountid,
    a.accountname,
    a.accountcode_alias,
    a.atype,
    b.alie,
    b.common_intergration_code,
    b.company,
    upper((b.primarygroup)::text) AS primarygroup
   FROM ({schema}.mg_account a
     RIGHT JOIN ( SELECT b_1.accountname,
            b_1.mg_accountid,
            b_1.alie,
            b_1.common_intergration_code,
            b_1.company,
            b_1.primarygroup
           FROM {schema}.mg_account b_1
          WHERE (b_1.common_intergration_code = ('1951220000000'::bigint)::numeric)) b ON (((a.groupname)::text = (b.accountname)::text)))
UNION
 SELECT 110 AS id,
    'LIABILITIES '::character varying AS category,
    b.mg_accountid AS groupid,
    b.accountname AS groupname,
    (COALESCE(a.mg_accountid, b.mg_accountid))::numeric AS mg_accountid,
    a.accountname,
    a.accountcode_alias,
    a.atype,
    b.alie,
    b.common_intergration_code,
    b.company,
    upper((b.primarygroup)::text) AS primarygroup
   FROM ({schema}.mg_account a
     RIGHT JOIN ( SELECT b_1.accountname,
            b_1.mg_accountid,
            b_1.alie,
            b_1.common_intergration_code,
            b_1.company,
            b_1.primarygroup
           FROM {schema}.mg_account b_1
          WHERE (b_1.common_intergration_code = ('1473330000000'::bigint)::numeric)) b ON (((a.groupname)::text = (b.accountname)::text)))
  ORDER BY 1;

CREATE VIEW {schema}.vw_cards_calendar_data AS
 SELECT DISTINCT a.uname,
    a.axcalendarid,
    a.eventname,
    a.sendername,
    a.messagetext,
    a.eventtype,
    a.startdate,
    COALESCE(a.axptm_starttime, '00:00'::character varying) AS axptm_starttime,
    a.enddate,
        CASE
            WHEN ((COALESCE(a.axptm_endtime, '00:00'::character varying))::text = '00:00'::text) THEN '23:59'::character varying
            ELSE a.axptm_endtime
        END AS axptm_endtime,
    a.location,
    a.status,
    b.eventcolor,
        CASE
            WHEN (a.sourceid = (0)::numeric) THEN a.axcalendarid
            ELSE a.sourceid
        END AS recordid,
    a.eventstatus,
    c.eventstatcolor,
    "substring"((a.mapname)::text, 1, 5) AS mapname
   FROM (({schema}.axcalendar a
     JOIN {schema}.axpdef_axcalendar_event b ON ((a.axpdef_axcalendar_eventid = b.axpdef_axcalendar_eventid)))
     LEFT JOIN ( SELECT axpdef_axcalendar_eventstatus.axpdef_axcalendar_eventid,
            axpdef_axcalendar_eventstatus.eventstatus,
            axpdef_axcalendar_eventstatus.eventstatcolor
           FROM {schema}.axpdef_axcalendar_eventstatus) c ON (((a.axpdef_axcalendar_eventid = c.axpdef_axcalendar_eventid) AND ((a.eventstatus)::text = (c.eventstatus)::text))))
  WHERE (((a.cancel)::text = 'F'::text) AND (a.parenteventid > (0)::numeric))
UNION ALL
 SELECT DISTINCT a.sendername AS uname,
    a.axcalendarid,
    a.eventname,
    a.sendername,
    a.messagetext,
    a.eventtype,
    a.startdate,
    COALESCE(a.axptm_starttime, '00:00'::character varying) AS axptm_starttime,
        CASE
            WHEN (a.recurring IS NULL) THEN a.enddate
            ELSE a.startdate
        END AS enddate,
        CASE
            WHEN ((COALESCE(a.axptm_endtime, '00:00'::character varying))::text = '00:00'::text) THEN '23:59'::character varying
            ELSE a.axptm_endtime
        END AS axptm_endtime,
    a.location,
    a.status,
    b.eventcolor,
        CASE
            WHEN (a.sourceid = (0)::numeric) THEN a.axcalendarid
            ELSE a.sourceid
        END AS recordid,
    a.eventstatus,
    c.eventstatcolor,
    "substring"((a.mapname)::text, 1, 5) AS mapname
   FROM (({schema}.axcalendar a
     JOIN {schema}.axpdef_axcalendar_event b ON ((a.axpdef_axcalendar_eventid = b.axpdef_axcalendar_eventid)))
     LEFT JOIN ( SELECT axpdef_axcalendar_eventstatus.axpdef_axcalendar_eventid,
            axpdef_axcalendar_eventstatus.eventstatus,
            axpdef_axcalendar_eventstatus.eventstatcolor
           FROM {schema}.axpdef_axcalendar_eventstatus) c ON (((a.axpdef_axcalendar_eventid = c.axpdef_axcalendar_eventid) AND ((a.eventstatus)::text = (c.eventstatus)::text))))
  WHERE (((a.cancel)::text = 'F'::text) AND (a.parenteventid = (0)::numeric))
  ORDER BY 7;

CREATE VIEW {schema}.vw_cards_dashboard AS
 SELECT a.cardtype,
    a.cardname,
    a.cardicon,
    a.charttype,
    a.pluginname,
        CASE
            WHEN (a.pluginname IS NULL) THEN a.html_editor_card
            ELSE h.htmltext
        END AS htmltext,
    a.card_datasource,
    s.sqltext,
    a.width,
    a.height,
    a.autorefresh,
    unnest(string_to_array((a.accessstringui)::text, ','::text)) AS uroles,
    a.axp_cardsid,
    h.context,
    a.orderno,
    a.chartjson
   FROM (({schema}.axp_cards a
     LEFT JOIN {schema}.ax_htmlplugins h ON (((a.pluginname)::text = (h.name)::text)))
     LEFT JOIN {schema}.axdirectsql s ON (((a.card_datasource)::text = (s.sqlname)::text)))
  WHERE ((a.indashboard)::text = 'T'::text);

CREATE VIEW {schema}.vw_cards_homepages AS
 SELECT a.cardtype,
    a.cardname,
    a.cardicon,
    a.charttype,
    a.pluginname,
        CASE
            WHEN (a.pluginname IS NULL) THEN a.html_editor_card
            ELSE h.htmltext
        END AS htmltext,
    a.card_datasource,
    s.sqltext,
    a.width,
    a.height,
    a.autorefresh,
    unnest(string_to_array((a.accessstringui)::text, ','::text)) AS uroles,
    a.axp_cardsid,
    h.context,
    a.orderno,
    a.chartjson
   FROM (({schema}.axp_cards a
     LEFT JOIN {schema}.ax_htmlplugins h ON (((a.pluginname)::text = (h.name)::text)))
     LEFT JOIN {schema}.axdirectsql s ON (((a.card_datasource)::text = (s.sqlname)::text)))
  WHERE ((a.inhomepage)::text = 'T'::text);

CREATE VIEW {schema}.vw_companycalendar AS
 SELECT a.company,
    to_char((a.startdate)::timestamp with time zone, 'YYYY'::text) AS financialyear,
    a.startdate,
    a.enddate,
    m.serialno,
    (a.startdate + ('1 mon'::interval * ((m.serialno - 1))::double precision)) AS monthstartdate,
    (((a.startdate + ('1 mon'::interval * ((m.serialno - 1))::double precision)) + '1 mon'::interval) - '1 day'::interval) AS monthenddate,
    to_char((a.startdate + ('1 mon'::interval * ((m.serialno - 1))::double precision)), 'Mon YYYY'::text) AS monthandyear,
        CASE
            WHEN ((m.serialno >= 1) AND (m.serialno <= 3)) THEN (a.startdate)::timestamp without time zone
            WHEN ((m.serialno >= 4) AND (m.serialno <= 6)) THEN (a.startdate + '3 mons'::interval)
            WHEN ((m.serialno >= 7) AND (m.serialno <= 9)) THEN (a.startdate + '6 mons'::interval)
            ELSE (a.startdate + '9 mons'::interval)
        END AS quarterstartdate,
        CASE
            WHEN ((m.serialno >= 1) AND (m.serialno <= 3)) THEN ((a.startdate + '3 mons'::interval) - '1 day'::interval)
            WHEN ((m.serialno >= 4) AND (m.serialno <= 6)) THEN ((a.startdate + '6 mons'::interval) - '1 day'::interval)
            WHEN ((m.serialno >= 7) AND (m.serialno <= 9)) THEN ((a.startdate + '9 mons'::interval) - '1 day'::interval)
            ELSE (a.enddate)::timestamp without time zone
        END AS quarterenddate,
        CASE
            WHEN ((m.serialno >= 1) AND (m.serialno <= 3)) THEN 'Q1'::text
            WHEN ((m.serialno >= 4) AND (m.serialno <= 6)) THEN 'Q2'::text
            WHEN ((m.serialno >= 6) AND (m.serialno <= 9)) THEN 'Q3'::text
            ELSE 'Q4'::text
        END AS quarter,
        CASE
            WHEN ((m.serialno >= 1) AND (m.serialno <= 6)) THEN (a.startdate)::timestamp without time zone
            ELSE (a.startdate + '6 mons'::interval)
        END AS halfyearlystartdate,
        CASE
            WHEN ((m.serialno >= 1) AND (m.serialno <= 6)) THEN ((a.startdate + '6 mons'::interval) - '1 day'::interval)
            ELSE (a.enddate)::timestamp without time zone
        END AS halfyearlyenddate,
        CASE
            WHEN ((m.serialno >= 1) AND (m.serialno <= 6)) THEN '1st Half'::text
            ELSE '2nd Half'::text
        END AS halfyear
   FROM ( SELECT a_1.companyid,
            b.financialyearid,
            b.cancel,
            b.sourceid,
            b.mapname,
            b.username,
            b.modifiedon,
            b.createdby,
            b.createdon,
            b.wkid,
            b.app_level,
            b.app_desc,
            b.app_slevel,
            b.cancelremarks,
            b.wfroles,
            b.company,
            b.startdate,
            b.mstartdate,
            b.enddate,
            b.finyr,
            b.finyrcode,
            b.finyridentifier,
            b.currentfinyr,
            b.active,
            b.closed
           FROM ({schema}.company a_1
             JOIN {schema}.financialyear b ON ((a_1.companyid = b.company)))
          WHERE ((b.finyr IS NOT NULL) AND ((b.cancel)::text = 'F'::text) AND ((COALESCE(b.active, 'F'::character varying))::text = 'T'::text) AND ((b.closed)::text = 'F'::text))) a,
    ( SELECT generate_series(1, 12) AS serialno
           FROM {schema}.dual) m
  ORDER BY a.company, a.finyr, a.startdate, m.serialno;

CREATE VIEW {schema}.vw_companycalendarrows AS
 WITH calendar_data AS (
         SELECT a_1.companyid,
            b.financialyearid,
            b.cancel,
            b.sourceid,
            b.mapname,
            b.username,
            b.modifiedon,
            b.createdby,
            b.createdon,
            b.wkid,
            b.app_level,
            b.app_desc,
            b.app_slevel,
            b.cancelremarks,
            b.wfroles,
            b.company,
            b.startdate,
            b.mstartdate,
            b.enddate,
            b.finyr,
            b.finyrcode,
            b.finyridentifier,
            b.currentfinyr,
            b.active,
            b.duplicatecheck,
            b.closed,
            m.serialno
           FROM {schema}.company a_1,
            {schema}.financialyear b,
            ( SELECT generate_series(1, 12) AS serialno
                   FROM {schema}.dual) m
          WHERE ((a_1.companyid = b.company) AND (b.finyr IS NOT NULL) AND ((b.cancel)::text = 'F'::text) AND ((COALESCE(b.active, 'F'::character varying))::text = 'T'::text) AND ((b.closed)::text = 'F'::text))
          ORDER BY b.company, b.finyr, b.startdate, m.serialno
        )
(
         SELECT a.company,
            a.finyr,
            to_char((a.startdate)::timestamp with time zone, 'YYYY'::text) AS financialyear,
            a.startdate,
            a.enddate,
            a.serialno,
            (a.startdate + ('1 mon'::interval * ((a.serialno - 1))::double precision)) AS period_startdate,
            (((a.startdate + ('1 mon'::interval * ((a.serialno - 1))::double precision)) + '1 mon'::interval) - '1 day'::interval) AS period_enddate,
            to_char((a.startdate + ('1 mon'::interval * ((a.serialno - 1))::double precision)), 'Mon YYYY'::text) AS period_text,
            'MONTHLY'::text AS period_range,
            (to_char((a.startdate + ('1 mon'::interval * ((a.serialno - 1))::double precision)), 'YYYYMM'::text))::numeric AS period_rangefrom,
            (to_char((((a.startdate + ('1 mon'::interval * ((a.serialno - 1))::double precision)) + '1 mon'::interval) - '1 day'::interval), 'YYYYMM'::text))::numeric AS period_rangeto,
            ((to_char((a.startdate + ('1 mon'::interval * ((a.serialno - 1))::double precision)), 'dd Mon'::text) || '-'::text) || to_char((((((a.startdate + ('1 mon'::interval * ((a.serialno - 1))::double precision)) + '1 mon'::interval) - '1 day'::interval))::date)::timestamp with time zone, 'dd Mon'::text)) AS period_subtext
           FROM calendar_data a
        UNION
         SELECT a.company,
            a.finyr,
            to_char((a.startdate)::timestamp with time zone, 'YYYY'::text) AS financialyear,
            a.startdate,
            a.enddate,
            a.serialno,
                CASE
                    WHEN (a.serialno <= 3) THEN (a.startdate)::timestamp without time zone
                    WHEN ((a.serialno >= 4) AND (a.serialno <= 6)) THEN (a.startdate + '3 mons'::interval)
                    WHEN ((a.serialno >= 7) AND (a.serialno <= 9)) THEN (a.startdate + '6 mons'::interval)
                    WHEN (a.serialno > 9) THEN (a.startdate + '9 mons'::interval)
                    ELSE NULL::timestamp without time zone
                END AS period_startdate,
                CASE
                    WHEN (a.serialno <= 3) THEN ((a.startdate + '3 mons'::interval) - '1 day'::interval)
                    WHEN ((a.serialno >= 4) AND (a.serialno <= 6)) THEN ((a.startdate + '6 mons'::interval) - '1 day'::interval)
                    WHEN ((a.serialno >= 7) AND (a.serialno <= 9)) THEN ((a.startdate + '9 mons'::interval) - '1 day'::interval)
                    WHEN (a.serialno > 9) THEN (a.enddate)::timestamp without time zone
                    ELSE NULL::timestamp without time zone
                END AS period_enddate,
            ((
                CASE
                    WHEN (a.serialno <= 3) THEN 'Q1'::text
                    WHEN ((a.serialno >= 4) AND (a.serialno <= 6)) THEN 'Q2'::text
                    WHEN ((a.serialno >= 6) AND (a.serialno <= 9)) THEN 'Q3'::text
                    WHEN (a.serialno > 9) THEN 'Q4'::text
                    ELSE NULL::text
                END || ' '::text) || to_char((a.startdate)::timestamp with time zone, 'YYYY'::text)) AS period_text,
            'QUARTER'::text AS period_range,
            (to_char(
                CASE
                    WHEN (a.serialno <= 3) THEN (a.startdate)::timestamp without time zone
                    WHEN ((a.serialno >= 4) AND (a.serialno <= 6)) THEN (a.startdate + '3 mons'::interval)
                    WHEN ((a.serialno >= 7) AND (a.serialno <= 9)) THEN (a.startdate + '6 mons'::interval)
                    WHEN (a.serialno > 9) THEN (a.startdate + '9 mons'::interval)
                    ELSE NULL::timestamp without time zone
                END, 'YYYYMM'::text))::numeric AS period_rangefrom,
            (to_char(
                CASE
                    WHEN (a.serialno <= 3) THEN ((a.startdate + '3 mons'::interval) - '1 day'::interval)
                    WHEN ((a.serialno >= 4) AND (a.serialno <= 6)) THEN ((a.startdate + '6 mons'::interval) - '1 day'::interval)
                    WHEN ((a.serialno >= 7) AND (a.serialno <= 9)) THEN ((a.startdate + '9 mons'::interval) - '1 day'::interval)
                    WHEN (a.serialno > 9) THEN (a.enddate)::timestamp without time zone
                    ELSE NULL::timestamp without time zone
                END, 'YYYYMM'::text))::numeric AS period_rangeto,
            ((to_char(
                CASE
                    WHEN (a.serialno <= 3) THEN (a.startdate)::timestamp without time zone
                    WHEN ((a.serialno >= 4) AND (a.serialno <= 6)) THEN (a.startdate + '3 mons'::interval)
                    WHEN ((a.serialno >= 7) AND (a.serialno <= 9)) THEN (a.startdate + '6 mons'::interval)
                    WHEN (a.serialno > 9) THEN (a.startdate + '9 mons'::interval)
                    ELSE NULL::timestamp without time zone
                END, 'dd Mon'::text) || ' - '::text) || to_char(
                CASE
                    WHEN (a.serialno <= 3) THEN ((a.startdate + '3 mons'::interval) - '1 day'::interval)
                    WHEN ((a.serialno >= 4) AND (a.serialno <= 6)) THEN ((a.startdate + '6 mons'::interval) - '1 day'::interval)
                    WHEN ((a.serialno >= 7) AND (a.serialno <= 9)) THEN ((a.startdate + '9 mons'::interval) - '1 day'::interval)
                    WHEN (a.serialno > 9) THEN (a.enddate)::timestamp without time zone
                    ELSE NULL::timestamp without time zone
                END, 'dd Mon'::text)) AS period_subtext
           FROM calendar_data a
) UNION ALL
 SELECT a.company,
    a.finyr,
    to_char((a.startdate)::timestamp with time zone, 'YYYY'::text) AS financialyear,
    a.startdate,
    a.enddate,
    a.serialno,
        CASE
            WHEN ((a.serialno >= 1) AND (a.serialno <= 6)) THEN (a.startdate)::timestamp without time zone
            ELSE (a.startdate + '6 mons'::interval)
        END AS period_startdate,
        CASE
            WHEN ((a.serialno >= 1) AND (a.serialno <= 6)) THEN ((a.startdate + '6 mons'::interval) - '1 day'::interval)
            ELSE (a.enddate)::timestamp without time zone
        END AS period_enddate,
    ((
        CASE
            WHEN ((a.serialno >= 1) AND (a.serialno <= 6)) THEN '1st Half'::text
            ELSE '2nd Half'::text
        END || ' '::text) || to_char((a.startdate)::timestamp with time zone, 'YYYY'::text)) AS period_text,
    'HALFYEARLY'::text AS period_range,
    (to_char(
        CASE
            WHEN ((a.serialno >= 1) AND (a.serialno <= 6)) THEN (a.startdate)::timestamp without time zone
            ELSE (a.startdate + '6 mons'::interval)
        END, 'YYYYMM'::text))::numeric AS period_rangefrom,
    (to_char(
        CASE
            WHEN ((a.serialno >= 1) AND (a.serialno <= 6)) THEN ((a.startdate + '6 mons'::interval) - '1 day'::interval)
            ELSE (a.enddate)::timestamp without time zone
        END, 'YYYYMM'::text))::numeric AS period_rangeto,
    ((to_char(
        CASE
            WHEN ((a.serialno >= 1) AND (a.serialno <= 6)) THEN (a.startdate)::timestamp without time zone
            ELSE (a.startdate + '6 mons'::interval)
        END, 'dd Mon'::text) || ' - '::text) || to_char(
        CASE
            WHEN ((a.serialno >= 1) AND (a.serialno <= 6)) THEN ((a.startdate + '6 mons'::interval) - '1 day'::interval)
            ELSE (a.enddate)::timestamp without time zone
        END, 'dd Mon'::text)) AS period_subtext
   FROM calendar_data a
  ORDER BY 1, 3, 10, 6;

CREATE VIEW {schema}.vw_companycalendarrowsv2 AS
 SELECT a.companyid,
    b.financialyearid,
    b.startdate,
    b.enddate,
    b.finyrcode,
    b.finyridentifier,
    b.active,
    b.closed,
    m.serialno,
    (b.startdate + ('1 mon'::interval * ((m.serialno - 1))::double precision)) AS period_startdate,
    (((b.startdate + ('1 mon'::interval * ((m.serialno - 1))::double precision)) + '1 mon'::interval) - '1 day'::interval) AS period_enddate,
    to_char((b.startdate + ('1 mon'::interval * ((m.serialno - 1))::double precision)), 'Mon YYYY'::text) AS period_text,
    ((to_char((b.startdate + ('1 mon'::interval * ((m.serialno - 1))::double precision)), 'dd Mon'::text) || '-'::text) || to_char((((b.startdate + ('1 mon'::interval * ((m.serialno - 1))::double precision)) + '1 mon'::interval) - '1 day'::interval), 'dd Mon'::text)) AS period_subtext,
    'MONTHLY'::text AS period_range
   FROM {schema}.company a,
    {schema}.financialyear b,
    ( SELECT generate_series(1, 12) AS serialno
           FROM {schema}.dual) m
  WHERE ((a.companyid = b.company) AND (b.finyr IS NOT NULL) AND ((b.cancel)::text = 'F'::text) AND ((COALESCE(b.active, 'F'::character varying))::text = 'T'::text) AND ((b.closed)::text = 'F'::text))
UNION ALL
 SELECT a.companyid,
    b.financialyearid,
    b.startdate,
    b.enddate,
    b.finyrcode,
    b.finyridentifier,
    b.active,
    b.closed,
    m.serialno,
    (b.startdate + ('3 mons'::interval * ((m.serialno - 1))::double precision)) AS period_startdate,
    (((b.startdate + ('3 mons'::interval * ((m.serialno - 1))::double precision)) + '3 mons'::interval) - '1 day'::interval) AS period_enddate,
    ((m.period_text || ' '::text) || (b.finyrcode)::text) AS period_text,
    ((to_char((b.startdate + ('3 mons'::interval * ((m.serialno - 1))::double precision)), 'dd Mon'::text) || '-'::text) || to_char((((b.startdate + ('3 mons'::interval * ((m.serialno - 1))::double precision)) + '3 mons'::interval) - '1 day'::interval), 'dd Mon'::text)) AS period_subtext,
    'QUARTERLY'::text AS period_range
   FROM {schema}.company a,
    {schema}.financialyear b,
    ( SELECT generate_series(1, 4) AS serialno,
            ('Q'::text || generate_series(1, 4)) AS period_text
           FROM {schema}.dual) m
  WHERE ((a.companyid = b.company) AND (b.finyr IS NOT NULL) AND ((b.cancel)::text = 'F'::text) AND ((COALESCE(b.active, 'F'::character varying))::text = 'T'::text) AND ((b.closed)::text = 'F'::text))
UNION ALL
 SELECT a.companyid,
    b.financialyearid,
    b.startdate,
    b.enddate,
    b.finyrcode,
    b.finyridentifier,
    b.active,
    b.closed,
    m.serialno,
    (b.startdate + ('3 mons'::interval * ((m.serialno - 1))::double precision)) AS period_startdate,
    (((b.startdate + ('3 mons'::interval * ((m.serialno - 1))::double precision)) + '3 mons'::interval) - '1 day'::interval) AS period_enddate,
    ((m.period_text || ' '::text) || (b.finyrcode)::text) AS period_text,
    ((to_char((b.startdate + ('6 mons'::interval * ((m.serialno - 1))::double precision)), 'dd Mon'::text) || '-'::text) || to_char((((b.startdate + ('6 mons'::interval * ((m.serialno - 1))::double precision)) + '6 mons'::interval) - '1 day'::interval), 'dd Mon'::text)) AS period_subtext,
    'HALFYEARLY'::text AS period_range
   FROM {schema}.company a,
    {schema}.financialyear b,
    ( SELECT generate_series(1, 2) AS serialno,
            ((generate_series(1, 2) || ' '::text) || 'Half'::text) AS period_text
           FROM {schema}.dual) m
  WHERE ((a.companyid = b.company) AND (b.finyr IS NOT NULL) AND ((b.cancel)::text = 'F'::text) AND ((COALESCE(b.active, 'F'::character varying))::text = 'T'::text) AND ((b.closed)::text = 'F'::text))
UNION ALL
 SELECT a.companyid,
    b.financialyearid,
    b.startdate,
    b.enddate,
    b.finyrcode,
    b.finyridentifier,
    b.active,
    b.closed,
    1 AS serialno,
    b.startdate AS period_startdate,
    b.enddate AS period_enddate,
    b.finyrcode AS period_text,
    ((to_char((b.startdate)::timestamp with time zone, 'dd Mon'::text) || '-'::text) || to_char((b.enddate)::timestamp with time zone, 'dd Mon'::text)) AS period_subtext,
    'YEARLY'::text AS period_range
   FROM {schema}.company a,
    {schema}.financialyear b
  WHERE ((a.companyid = b.company) AND (b.finyr IS NOT NULL) AND ((b.cancel)::text = 'F'::text) AND ((COALESCE(b.active, 'F'::character varying))::text = 'T'::text) AND ((b.closed)::text = 'F'::text))
  ORDER BY 1, 5, 3, 9;

CREATE VIEW {schema}.vw_entityrelations AS
 SELECT DISTINCT nextval('{schema}.ax_entity_relseq'::regclass) AS axentityrelationsid,
    'F'::text AS cancel,
    'admin'::text AS username,
    CURRENT_TIMESTAMP AS modifiedon,
    'admin'::text AS createdby,
    CURRENT_TIMESTAMP AS createdon,
    1 AS app_level,
    1 AS app_desc,
    a.rtype,
    a.mstruct,
    a.mfield,
    m.tablename AS mtable,
    dc.tablename AS primarytable,
    a.dstruct,
    a.dfield,
    d.tablename AS dtable,
    'Dropdown'::text AS rtypeui,
    concat(mt.caption, '-(', mt.name, ')') AS mstructui,
    concat(m.caption, '-(', m.fname, ')') AS mfieldui,
    concat(dt.caption, '-(', dt.name, ')') AS dstructui,
    concat(d.caption, '-(', d.fname, ')') AS dfieldui,
    ddc.tablename AS dprimarytable
   FROM ((((((( SELECT DISTINCT axrelations.mstruct,
            axrelations.dstruct,
            axrelations.mfield,
            axrelations.dfield,
            axrelations.rtype
           FROM {schema}.axrelations) a
     JOIN {schema}.tstructs mt ON (((a.mstruct)::text = (mt.name)::text)))
     JOIN {schema}.tstructs dt ON (((a.dstruct)::text = (dt.name)::text)))
     LEFT JOIN {schema}.axpflds m ON ((((a.mstruct)::text = (m.tstruct)::text) AND ((a.mfield)::text = (m.fname)::text))))
     LEFT JOIN {schema}.axpflds d ON ((((a.dstruct)::text = (d.tstruct)::text) AND ((a.dfield)::text = (d.fname)::text))))
     LEFT JOIN {schema}.axpdc dc ON ((((a.mstruct)::text = (dc.tstruct)::text) AND ((dc.dname)::text = 'dc1'::text))))
     LEFT JOIN {schema}.axpdc ddc ON ((((a.dstruct)::text = (ddc.tstruct)::text) AND ((ddc.dname)::text = 'dc1'::text))))
  WHERE ((a.rtype)::text = 'md'::text)
UNION ALL
 SELECT DISTINCT nextval('{schema}.ax_entity_relseq'::regclass) AS axentityrelationsid,
    'F'::text AS cancel,
    'admin'::text AS username,
    CURRENT_TIMESTAMP AS modifiedon,
    'admin'::text AS createdby,
    CURRENT_TIMESTAMP AS createdon,
    1 AS app_level,
    1 AS app_desc,
    'gm'::character varying AS rtype,
    a.tstruct AS mstruct,
    concat(sd.tablename, 'id') AS mfield,
    sd.tablename AS mtable,
    pd.tablename AS primarytable,
    a.targettstr AS dstruct,
    'sourceid'::character varying AS dfield,
    td.tablename AS dtable,
    'Genmap'::text AS rtypeui,
    concat(mt.caption, '-(', mt.name, ')') AS mstructui,
    NULL::text AS mfieldui,
    concat(dt.caption, '-(', dt.name, ')') AS dstructui,
    NULL::text AS dfieldui,
    td.tablename AS dprimarytable
   FROM ((((({schema}.axpgenmaps a
     JOIN {schema}.tstructs mt ON (((a.tstruct)::text = (mt.name)::text)))
     JOIN {schema}.tstructs dt ON (((a.targettstr)::text = (dt.name)::text)))
     LEFT JOIN {schema}.axpdc sd ON ((((a.tstruct)::text = (sd.tstruct)::text) AND ((sd.dname)::text = (a.basedondc)::text))))
     LEFT JOIN {schema}.axpdc td ON ((((a.targettstr)::text = (td.tstruct)::text) AND ((td.dname)::text = 'dc1'::text))))
     LEFT JOIN {schema}.axpdc pd ON ((((a.tstruct)::text = (pd.tstruct)::text) AND ((pd.dname)::text = 'dc1'::text))));

CREATE VIEW {schema}.vw_financialyear AS
 SELECT company,
    startdate,
    enddate,
    finyr,
    finyrcode,
    active,
    (((EXTRACT(year FROM startdate))::text || lpad((EXTRACT(month FROM startdate))::text, 2, '0'::text)))::numeric AS startdatenumeric,
    (((EXTRACT(year FROM enddate))::text || lpad((EXTRACT(month FROM enddate))::text, 2, '0'::text)))::numeric AS enddatenumeric
   FROM {schema}.financialyear a;

CREATE VIEW {schema}.vw_grnitems_salesorder AS
 SELECT pra.salesorder_itemsid,
    pra.salesorder_headerid,
    pra.salesorder_number,
    b.itemname,
    b.stocking_qty AS qty
   FROM {schema}.grn_header a,
    {schema}.grn_items b,
    {schema}.po_header poa,
    {schema}.po_items pob,
    {schema}.purrqhdr pra
  WHERE ((a.grn_headerid = b.grn_headerid) AND ((a.cancel)::text = 'F'::text) AND (a.app_desc = (1)::numeric) AND ((b.podocid)::text = (poa.docid)::text) AND ((poa.cancel)::text = 'F'::text) AND (poa.app_desc = (1)::numeric) AND (b.po_itemsid = pob.po_itemsid) AND (poa.po_headerid = pob.po_headerid) AND (pob.purrqdtlid = pra.purrqhdrid) AND ((pra.cancel)::text = 'F'::text) AND (pra.app_desc = (1)::numeric));

CREATE VIEW {schema}.vw_invoicebatchallocation AS
 WITH RECURSIVE ordered_batches AS (
         SELECT bs.itemname,
            bs.batch,
            bs.location,
            ((bs.inward - bs.outward) - bs.reserved_qty) AS stockqty,
            bs.createdon,
            so.salesorder_itemsid,
            so.order_qty,
            so.salesorder_headerid,
            so.invoiced_qty,
            row_number() OVER (PARTITION BY bs.location, so.salesorder_itemsid ORDER BY bs.createdon) AS rn
           FROM ({schema}.salesorder_items so
             JOIN {schema}.batchmaster bs ON ((bs.itemname = so.itemname)))
          WHERE ((so.order_qty - so.invoiced_qty) > (0)::numeric)
        ), recursive_allocation AS (
         SELECT ob.salesorder_itemsid,
            ob.salesorder_headerid,
            ob.itemname,
            ob.location,
            ob.batch,
            ob.createdon,
            (ob.order_qty - ob.invoiced_qty) AS remaining_qty,
            ob.stockqty,
            LEAST(ob.order_qty, ob.stockqty) AS allocated_qty,
            ob.rn,
            ob.invoiced_qty
           FROM ordered_batches ob
          WHERE (ob.rn = 1)
        UNION ALL
         SELECT ob.salesorder_itemsid,
            ob.salesorder_headerid,
            ob.itemname,
            ob.location,
            ob.batch,
            ob.createdon,
            (ra.remaining_qty - ra.allocated_qty) AS remaining_qty,
            ob.stockqty,
            LEAST(ob.stockqty, (ra.remaining_qty - ra.allocated_qty)) AS allocated_qty,
            ob.rn,
            ob.invoiced_qty
           FROM (ordered_batches ob
             JOIN recursive_allocation ra ON (((ob.salesorder_itemsid = ra.salesorder_itemsid) AND (ob.location = ra.location) AND (ob.rn = (ra.rn + 1)))))
          WHERE (ra.remaining_qty > ra.allocated_qty)
        )
 SELECT a.salesorder_itemsid,
    a.salesorder_headerid,
    a.itemname,
    a.location,
    a.batch,
    a.createdon,
    a.remaining_qty,
    a.stockqty,
    a.allocated_qty,
    a.rn,
    a.invoiced_qty,
    sh.docid
   FROM (recursive_allocation a
     JOIN {schema}.salesorder_header sh ON ((a.salesorder_headerid = sh.salesorder_headerid)))
  WHERE (NOT (EXISTS ( SELECT 'x'::text AS text
           FROM {schema}.deliverychallanhdr c
          WHERE ((c.salesorder_number)::text = (sh.docid)::text))));

CREATE VIEW {schema}.vw_item AS
 SELECT a.itemid,
    a.cancel,
    a.sourceid,
    a.mapname,
    a.username,
    a.modifiedon,
    a.createdby,
    a.createdon,
    a.wkid,
    a.app_level,
    a.app_desc,
    a.app_slevel,
    a.cancelremarks,
    a.wfroles,
    a.isbillable,
    a.isfixedasset,
    a.itemcode,
    a.itemname,
    a.expiryapp,
    a.itemval,
    a.valmethod,
    a.active,
    a.transid,
    a.company,
    a.primaryuomsales,
    b.code AS hsnno,
    a.itemcategory,
    a.uom,
    a.taxcategorycode,
    a.leadtime,
    a.stdsellingprice,
    a.minorderqty,
    a.reorderqty,
    a.type,
    a.itemtype,
    a.isserial,
    a.inventorytype,
    a.isinventory,
    a.pricing,
    a.item_hsn,
    a.hsntaxrate,
    a.fg,
    NULL::text AS psize,
    NULL::text AS pcolour,
    NULL::text AS pothers,
    a.itembrand,
    a.brandcode,
    a.itemdesc,
    a.codemanual,
    a.taxableyn,
    COALESCE(a.allowdecqty, 'F'::character varying) AS allowdecimalqty,
    a.attributes,
    a.stockingunit,
    a.purchaseunit,
    a.conversion_stockpurchase,
    a.sellingunit,
    a.conversion_stocksales,
    COALESCE(a.comboitem, 'F'::character varying) AS comboitem,
    a.valuationmethod,
    a.salesac,
    a.stockac,
    a.purchaseac,
    a.weight,
    d.productcategory,
    t.rate AS taxper,
    t.accountname AS taxac,
    a.invmethod,
    a.customer_warranty_enabled,
    a.warranty_period,
    a.legacyitemcode,
    a.isbom,
    a.purchaseac AS import_purchasac,
    a.salesac AS export_salesac,
    c.categoryname,
    c.itemcategoryid,
    d.productcategoryid,
    a.inventorymethod
   FROM (((({schema}.item a
     LEFT JOIN {schema}.hsnsac_codes b ON (((a.hsnno = b.hsnsac_codesid) AND ((a.company = b.company) OR (b.hsnsac_codesid = (1)::numeric)))))
     LEFT JOIN {schema}.taxmaster t ON ((((b.taxcode)::text = (t.taxcode)::text) AND (a.company = t.company))))
     LEFT JOIN {schema}.itemcategory c ON ((a.itemcategory = c.itemcategoryid)))
     LEFT JOIN {schema}.productcategory d ON ((a.productcategory = d.productcategoryid)))
  WHERE (((a.cancel)::bpchar = 'F'::bpchar) AND ((a.active)::text = 'T'::text))
  ORDER BY a.itemdesc;

CREATE VIEW {schema}.vw_item_hsncode AS
 SELECT b.code,
    '1'::text AS id,
    a.effective_from,
    b.taxcode,
    b.taxableyn,
    t.rate,
    a.company,
    a.itemname
   FROM {schema}.hsntaxmapping a,
    {schema}.hsnsac_codes b,
    {schema}.taxmaster t
  WHERE (((a.cancel)::text = 'F'::text) AND (a.hsnsaccode = b.hsnsac_codesid) AND ((b.taxcode)::text = (t.taxcode)::text))
UNION ALL
 SELECT b.code,
    '2'::text AS id,
    a.effective_from,
    b.taxcode,
    b.taxableyn,
    t.rate,
    a.company,
    i.itemname
   FROM {schema}.hsntaxmapping a,
    {schema}.hsnsac_codes b,
    {schema}.vw_item i,
    {schema}.taxmaster t
  WHERE (((a.cancel)::text = 'F'::text) AND ((a.itemcategory)::text = (i.categoryname)::text) AND ((a.productcategory)::text = (i.productcategory)::text) AND ((a.itemname)::text = 'ALL'::text) AND ((a.productcategory)::text <> 'ALL'::text) AND (a.hsnsaccode = b.hsnsac_codesid) AND ((b.taxcode)::text = (t.taxcode)::text))
UNION ALL
 SELECT b.code,
    '3'::text AS id,
    a.effective_from,
    b.taxcode,
    b.taxableyn,
    t.rate,
    a.company,
    i.itemname
   FROM {schema}.hsntaxmapping a,
    {schema}.hsnsac_codes b,
    {schema}.vw_item i,
    {schema}.taxmaster t
  WHERE (((a.cancel)::text = 'F'::text) AND ((a.itemcategory)::text = (i.categoryname)::text) AND ((a.productcategory)::text = 'ALL'::text) AND ((a.itemname)::text = 'ALL'::text) AND (a.hsnsaccode = b.hsnsac_codesid) AND ((b.taxcode)::text = (t.taxcode)::text))
UNION ALL
 SELECT vi.hsnno AS code,
    '4'::text AS id,
    date(vi.createdon) AS effective_from,
    vi.taxcategorycode AS taxcode,
    vi.taxableyn,
    vi.taxper AS rate,
    vi.company,
    vi.itemname
   FROM {schema}.vw_item vi;

CREATE VIEW {schema}.vw_pandlsetup AS
 SELECT 10 AS id,
    'TURNOVER'::character varying AS category,
    b.mg_accountid AS groupid,
    b.accountname AS groupname,
    (COALESCE(a.mg_accountid, b.mg_accountid))::numeric AS mg_accountid,
    a.accountname,
    a.accountcode_alias,
    a.atype,
    b.alie,
    b.common_intergration_code,
    b.company,
    upper((b.primarygroup)::text) AS primarygroup
   FROM ({schema}.mg_account a
     RIGHT JOIN ( SELECT b_1.accountname,
            b_1.mg_accountid,
            b_1.alie,
            b_1.common_intergration_code,
            b_1.company,
            b_1.primarygroup
           FROM {schema}.mg_account b_1
          WHERE (b_1.common_intergration_code = ('1359880000000'::bigint)::numeric)) b ON (((a.groupname)::text = (b.accountname)::text)))
UNION ALL
 SELECT 20 AS id,
    'TURNOVER'::character varying AS category,
    b.mg_accountid AS groupid,
    b.accountname AS groupname,
    (COALESCE(a.mg_accountid, b.mg_accountid))::numeric AS mg_accountid,
    a.accountname,
    a.accountcode_alias,
    a.atype,
    b.alie,
    b.common_intergration_code,
    b.company,
    upper((b.primarygroup)::text) AS primarygroup
   FROM ({schema}.mg_account a
     RIGHT JOIN ( SELECT b_1.accountname,
            b_1.mg_accountid,
            b_1.alie,
            b_1.common_intergration_code,
            b_1.company,
            b_1.primarygroup
           FROM {schema}.mg_account b_1
          WHERE (b_1.common_intergration_code = ('1359990000000'::bigint)::numeric)) b ON (((a.groupname)::text = (b.accountname)::text)))
UNION ALL
 SELECT 30 AS id,
    'COST OF SALES'::character varying AS category,
    b.mg_accountid AS groupid,
    b.accountname AS groupname,
    (COALESCE(a.mg_accountid, b.mg_accountid))::numeric AS mg_accountid,
    a.accountname,
    a.accountcode_alias,
    a.atype,
    b.alie,
    b.common_intergration_code,
    b.company,
    upper((b.primarygroup)::text) AS primarygroup
   FROM ({schema}.mg_account a
     RIGHT JOIN ( SELECT b_1.accountname,
            b_1.mg_accountid,
            b_1.alie,
            b_1.common_intergration_code,
            b_1.company,
            b_1.primarygroup
           FROM {schema}.mg_account b_1
          WHERE (b_1.common_intergration_code = ('1360330000000'::bigint)::numeric)) b ON (((a.groupname)::text = (b.accountname)::text)))
UNION ALL
 SELECT 40 AS id,
    'COST OF SALES'::character varying AS category,
    b.mg_accountid AS groupid,
    b.accountname AS groupname,
    (COALESCE(a.mg_accountid, b.mg_accountid))::numeric AS mg_accountid,
    a.accountname,
    a.accountcode_alias,
    a.atype,
    b.alie,
    b.common_intergration_code,
    b.company,
    upper((b.primarygroup)::text) AS primarygroup
   FROM ({schema}.mg_account a
     RIGHT JOIN ( SELECT b_1.accountname,
            b_1.mg_accountid,
            b_1.alie,
            b_1.common_intergration_code,
            b_1.company,
            b_1.primarygroup
           FROM {schema}.mg_account b_1
          WHERE (b_1.common_intergration_code = ('1360440000000'::bigint)::numeric)) b ON (((a.groupname)::text = (b.accountname)::text)))
UNION ALL
 SELECT 60 AS id,
    'COST OF SALES'::character varying AS category,
    b.mg_accountid AS groupid,
    b.accountname AS groupname,
    (COALESCE(a.mg_accountid, b.mg_accountid))::numeric AS mg_accountid,
    a.accountname,
    a.accountcode_alias,
    a.atype,
    b.alie,
    b.common_intergration_code,
    b.company,
    upper((b.primarygroup)::text) AS primarygroup
   FROM ({schema}.mg_account a
     RIGHT JOIN ( SELECT b_1.accountname,
            b_1.mg_accountid,
            b_1.alie,
            b_1.common_intergration_code,
            b_1.company,
            b_1.primarygroup
           FROM {schema}.mg_account b_1
          WHERE (b_1.common_intergration_code = ('1474550000000'::bigint)::numeric)) b ON (((a.groupname)::text = (b.accountname)::text)))
UNION ALL
 SELECT 70 AS id,
    NULL::character varying AS category,
    b.mg_accountid AS groupid,
    NULL::character varying AS groupname,
    (COALESCE(a.mg_accountid, b.mg_accountid))::numeric AS mg_accountid,
    'GROSS PROFIT / (LOSS)'::character varying AS accountname,
    a.accountcode_alias,
    a.atype,
    b.alie,
    b.common_intergration_code,
    b.company,
    upper((b.primarygroup)::text) AS primarygroup
   FROM ({schema}.mg_account a
     RIGHT JOIN ( SELECT b_1.accountname,
            b_1.mg_accountid,
            b_1.alie,
            b_1.common_intergration_code,
            b_1.company,
            b_1.primarygroup
           FROM {schema}.mg_account b_1
          WHERE (b_1.common_intergration_code = ANY (ARRAY[('1359880000000'::bigint)::numeric, ('1359990000000'::bigint)::numeric, ('1360330000000'::bigint)::numeric, ('1360440000000'::bigint)::numeric, ('1474550000000'::bigint)::numeric]))) b ON (((a.groupname)::text = (b.accountname)::text)))
UNION ALL
 SELECT 80 AS id,
    'OTHER INCOME'::character varying AS category,
    b.mg_accountid AS groupid,
    b.accountname AS groupname,
    (COALESCE(a.mg_accountid, b.mg_accountid))::numeric AS mg_accountid,
    a.accountname,
    a.accountcode_alias,
    a.atype,
    b.alie,
    b.common_intergration_code,
    b.company,
    upper((b.primarygroup)::text) AS primarygroup
   FROM ({schema}.mg_account a
     RIGHT JOIN ( SELECT b_1.accountname,
            b_1.mg_accountid,
            b_1.alie,
            b_1.common_intergration_code,
            b_1.company,
            b_1.primarygroup
           FROM {schema}.mg_account b_1
          WHERE (b_1.common_intergration_code = ('1360010000000'::bigint)::numeric)) b ON (((a.groupname)::text = (b.accountname)::text)))
UNION ALL
 SELECT 90 AS id,
    'EXPENSES'::character varying AS category,
    b.mg_accountid AS groupid,
    b.accountname AS groupname,
    (COALESCE(a.mg_accountid, b.mg_accountid))::numeric AS mg_accountid,
    a.accountname,
    a.accountcode_alias,
    a.atype,
    b.alie,
    b.common_intergration_code,
    b.company,
    upper((b.primarygroup)::text) AS primarygroup
   FROM ({schema}.mg_account a
     RIGHT JOIN ( SELECT b_1.accountname,
            b_1.mg_accountid,
            b_1.alie,
            b_1.common_intergration_code,
            b_1.company,
            b_1.primarygroup
           FROM {schema}.mg_account b_1
          WHERE (b_1.common_intergration_code = ('1360550000000'::bigint)::numeric)) b ON (((a.groupname)::text = (b.accountname)::text)))
UNION ALL
 SELECT 100 AS id,
    'EXPENSES'::character varying AS category,
    b.mg_accountid AS groupid,
    b.accountname AS groupname,
    (COALESCE(a.mg_accountid, b.mg_accountid))::numeric AS mg_accountid,
    a.accountname,
    a.accountcode_alias,
    a.atype,
    b.alie,
    b.common_intergration_code,
    b.company,
    upper((b.primarygroup)::text) AS primarygroup
   FROM ({schema}.mg_account a
     RIGHT JOIN ( SELECT b_1.accountname,
            b_1.mg_accountid,
            b_1.alie,
            b_1.common_intergration_code,
            b_1.company,
            b_1.primarygroup
           FROM {schema}.mg_account b_1
          WHERE (b_1.common_intergration_code = ('1360660000000'::bigint)::numeric)) b ON (((a.groupname)::text = (b.accountname)::text)))
UNION ALL
 SELECT 110 AS id,
    'EXPENSES'::character varying AS category,
    b.mg_accountid AS groupid,
    b.accountname AS groupname,
    (COALESCE(a.mg_accountid, b.mg_accountid))::numeric AS mg_accountid,
    a.accountname,
    a.accountcode_alias,
    a.atype,
    b.alie,
    b.common_intergration_code,
    b.company,
    upper((b.primarygroup)::text) AS primarygroup
   FROM ({schema}.mg_account a
     RIGHT JOIN ( SELECT b_1.accountname,
            b_1.mg_accountid,
            b_1.alie,
            b_1.common_intergration_code,
            b_1.company,
            b_1.primarygroup
           FROM {schema}.mg_account b_1
          WHERE (b_1.common_intergration_code = ('1360770000000'::bigint)::numeric)) b ON (((a.groupname)::text = (b.accountname)::text)))
UNION ALL
 SELECT 120 AS id,
    'EXPENSES'::character varying AS category,
    b.mg_accountid AS groupid,
    b.accountname AS groupname,
    (COALESCE(a.mg_accountid, b.mg_accountid))::numeric AS mg_accountid,
    a.accountname,
    a.accountcode_alias,
    a.atype,
    b.alie,
    b.common_intergration_code,
    b.company,
    upper((b.primarygroup)::text) AS primarygroup
   FROM ({schema}.mg_account a
     RIGHT JOIN ( SELECT b_1.accountname,
            b_1.mg_accountid,
            b_1.alie,
            b_1.common_intergration_code,
            b_1.company,
            b_1.primarygroup
           FROM {schema}.mg_account b_1
          WHERE (b_1.common_intergration_code = ('1360880000000'::bigint)::numeric)) b ON (((a.groupname)::text = (b.accountname)::text)))
UNION ALL
 SELECT 130 AS id,
    NULL::character varying AS category,
    b.mg_accountid AS groupid,
    NULL::character varying AS groupname,
    (COALESCE(a.mg_accountid, b.mg_accountid))::numeric AS mg_accountid,
    'NET PROFIT / (LOSS)'::character varying AS accountname,
    a.accountcode_alias,
    a.atype,
    b.alie,
    b.common_intergration_code,
    b.company,
    upper((b.primarygroup)::text) AS primarygroup
   FROM ({schema}.mg_account a
     RIGHT JOIN ( SELECT b_1.accountname,
            b_1.mg_accountid,
            b_1.alie,
            b_1.common_intergration_code,
            b_1.company,
            b_1.primarygroup
           FROM {schema}.mg_account b_1
          WHERE (b_1.common_intergration_code = ANY (ARRAY[('1359880000000'::bigint)::numeric, ('1359990000000'::bigint)::numeric, ('1360330000000'::bigint)::numeric, ('1360440000000'::bigint)::numeric, ('1474550000000'::bigint)::numeric, ('1360010000000'::bigint)::numeric, ('1360660000000'::bigint)::numeric, ('1360550000000'::bigint)::numeric, ('1360660000000'::bigint)::numeric, ('1360770000000'::bigint)::numeric, ('1360880000000'::bigint)::numeric]))) b ON (((a.groupname)::text = (b.accountname)::text)))
  ORDER BY 1;

CREATE VIEW {schema}.vw_pegv2_activetasks AS
 SELECT DISTINCT a.touser,
    a.processname,
    a.taskname,
    a.taskid,
    a.tasktype,
    a.eventdatetime AS edatetime,
    to_char(to_timestamp((a.eventdatetime)::text, 'YYYYMMDDHH24MISSSSS'::text), 'dd/mm/yyyy hh24:mi:ss'::text) AS eventdatetime,
    a.fromuser,
    a.fromrole,
    a.displayicon,
    a.displaytitle,
    a.displaymcontent,
    a.displaycontent,
    a.displaybuttons,
    a.keyfield,
    a.keyvalue,
    a.transid,
    a.priorindex,
    a.indexno,
    a.subindexno,
    a.approvereasons,
    a.defapptext,
    a.returnreasons,
    a.defrettext,
    a.rejectreasons,
    a.defregtext,
    aa.recordid,
    a.approvalcomments,
    a.rejectcomments,
    a.returncomments,
    'PEG'::text AS rectype,
    'NA'::text AS msgtype,
    a.returnable,
    a.initiator,
    a.initiator_approval,
    a.displaysubtitle,
    p.amendment,
    a.allowsend,
    a.allowsendflg,
    b.cmsg_appcheck,
    b.cmsg_return,
    b.cmsg_reject,
    b.showbuttons,
    NULL::text AS hlink,
    NULL::text AS hlink_transid,
    NULL::text AS hlink_params
   FROM ((({schema}.axactivetasks a
     JOIN {schema}.axprocessdefv2 b ON ((((a.processname)::text = (b.processname)::text) AND ((a.taskname)::text = (b.taskname)::text))))
     JOIN {schema}.axpdef_peg_processmaster p ON (((a.processname)::text = (p.caption)::text)))
     LEFT JOIN {schema}.axactivetasks aa ON ((((a.processname)::text = (aa.processname)::text) AND ((a.keyvalue)::text = (aa.keyvalue)::text) AND ((a.transid)::text = (aa.transid)::text) AND ((aa.tasktype)::text = 'Make'::text) AND (aa.recordid IS NOT NULL))))
  WHERE ((NOT (EXISTS ( SELECT b_1.taskid
           FROM {schema}.axactivetaskstatus b_1
          WHERE ((a.taskid)::text = (b_1.taskid)::text)))) AND ((a.removeflg)::text = 'F'::text))
UNION ALL
 SELECT axactivemessages.touser,
    axactivemessages.processname,
    axactivemessages.taskname,
    axactivemessages.taskid,
    axactivemessages.tasktype,
    axactivemessages.eventdatetime AS edatetime,
    to_char(to_timestamp((axactivemessages.eventdatetime)::text, 'YYYYMMDDHH24MISSSSS'::text), 'dd/mm/yyyy hh24:mi:ss'::text) AS eventdatetime,
    axactivemessages.fromuser,
    NULL::character varying AS fromrole,
    axactivemessages.displayicon,
    axactivemessages.displaytitle,
    NULL::text AS displaymcontent,
    axactivemessages.displaycontent,
    NULL::character varying AS displaybuttons,
    axactivemessages.keyfield,
    axactivemessages.keyvalue,
    axactivemessages.transid,
    0 AS priorindex,
    axactivemessages.indexno,
    0 AS subindexno,
    NULL::character varying AS approvereasons,
    NULL::character varying AS defapptext,
    NULL::character varying AS returnreasons,
    NULL::character varying AS defrettext,
    NULL::character varying AS rejectreasons,
    NULL::character varying AS defregtext,
    0 AS recordid,
    NULL::character varying AS approvalcomments,
    NULL::character varying AS rejectcomments,
    NULL::character varying AS returncomments,
    'MSG'::text AS rectype,
    axactivemessages.msgtype,
    'F'::character varying AS returnable,
    NULL::character varying AS initiator,
    NULL::character varying AS initiator_approval,
    NULL::character varying AS displaysubtitle,
    p.amendment,
    'F'::character varying AS allowsend,
    'F'::character varying AS allowsendflg,
    NULL::text AS cmsg_appcheck,
    NULL::text AS cmsg_return,
    NULL::text AS cmsg_reject,
    NULL::character varying AS showbuttons,
    axactivemessages.hlink,
    axactivemessages.hlink_transid,
    axactivemessages.hlink_params
   FROM ({schema}.axactivemessages
     LEFT JOIN {schema}.axpdef_peg_processmaster p ON (((axactivemessages.processname)::text = (p.caption)::text)))
  WHERE (NOT (EXISTS ( SELECT b.taskid
           FROM {schema}.axactivetaskstatus b
          WHERE ((axactivemessages.taskid)::text = (b.taskid)::text))));

CREATE VIEW {schema}.vw_pegv2_alltasks AS
 SELECT DISTINCT a.touser,
    a.processname,
    a.taskname,
    a.taskid,
    a.tasktype,
    a.eventdatetime AS edatetime,
    to_char(to_timestamp((a.eventdatetime)::text, 'YYYYMMDDHH24MISSSSS'::text), 'dd/mm/yyyy hh24:mi:ss'::text) AS eventdatetime,
    a.fromuser,
    a.fromrole,
    a.displayicon,
    a.displaytitle,
    a.displaymcontent,
    a.displaycontent,
    a.displaybuttons,
    a.keyfield,
    a.keyvalue,
    a.transid,
    a.priorindex,
    a.indexno,
    a.subindexno,
    a.approvereasons,
    a.defapptext,
    a.returnreasons,
    a.defrettext,
    a.rejectreasons,
    a.defregtext,
    aa.recordid,
    a.approvalcomments,
    a.rejectcomments,
    a.returncomments,
    'PEG'::text AS rectype,
    'NA'::text AS msgtype,
    a.returnable,
    a.initiator,
    a.initiator_approval,
    a.displaysubtitle,
    p.amendment,
    a.allowsend,
    a.allowsendflg,
    b.cmsg_appcheck,
    b.cmsg_return,
    b.cmsg_reject,
    b.showbuttons,
    NULL::text AS hlink,
    NULL::text AS hlink_transid,
    NULL::text AS hlink_params,
    NULL::text AS taskstatus,
    NULL::text AS statusreason,
    NULL::text AS statustext,
    NULL::text AS cancelremarks,
    NULL::text AS cancelledby,
    NULL::text AS cancelledon,
    NULL::text AS cancel,
    NULL::text AS username,
    'Active'::text AS cstatus
   FROM ((({schema}.axactivetasks a
     JOIN {schema}.axprocessdefv2 b ON ((((a.processname)::text = (b.processname)::text) AND ((a.taskname)::text = (b.taskname)::text))))
     JOIN {schema}.axpdef_peg_processmaster p ON (((a.processname)::text = (p.caption)::text)))
     LEFT JOIN {schema}.axactivetasks aa ON ((((a.processname)::text = (aa.processname)::text) AND ((a.keyvalue)::text = (aa.keyvalue)::text) AND ((a.transid)::text = (aa.transid)::text) AND ((aa.tasktype)::text = 'Make'::text) AND (aa.recordid IS NOT NULL))))
  WHERE ((NOT (EXISTS ( SELECT b_1.taskid
           FROM {schema}.axactivetaskstatus b_1
          WHERE ((a.taskid)::text = (b_1.taskid)::text)))) AND ((a.removeflg)::text = 'F'::text))
UNION ALL
 SELECT axactivemessages.touser,
    axactivemessages.processname,
    axactivemessages.taskname,
    axactivemessages.taskid,
    axactivemessages.tasktype,
    axactivemessages.eventdatetime AS edatetime,
    to_char(to_timestamp((axactivemessages.eventdatetime)::text, 'YYYYMMDDHH24MISSSSS'::text), 'dd/mm/yyyy hh24:mi:ss'::text) AS eventdatetime,
    axactivemessages.fromuser,
    NULL::character varying AS fromrole,
    axactivemessages.displayicon,
    axactivemessages.displaytitle,
    NULL::text AS displaymcontent,
    axactivemessages.displaycontent,
    NULL::character varying AS displaybuttons,
    axactivemessages.keyfield,
    axactivemessages.keyvalue,
    axactivemessages.transid,
    0 AS priorindex,
    axactivemessages.indexno,
    0 AS subindexno,
    NULL::character varying AS approvereasons,
    NULL::character varying AS defapptext,
    NULL::character varying AS returnreasons,
    NULL::character varying AS defrettext,
    NULL::character varying AS rejectreasons,
    NULL::character varying AS defregtext,
    0 AS recordid,
    NULL::character varying AS approvalcomments,
    NULL::character varying AS rejectcomments,
    NULL::character varying AS returncomments,
    'MSG'::text AS rectype,
    axactivemessages.msgtype,
    'F'::character varying AS returnable,
    NULL::character varying AS initiator,
    NULL::character varying AS initiator_approval,
    NULL::character varying AS displaysubtitle,
    p.amendment,
    'F'::character varying AS allowsend,
    'F'::character varying AS allowsendflg,
    NULL::text AS cmsg_appcheck,
    NULL::text AS cmsg_return,
    NULL::text AS cmsg_reject,
    NULL::character varying AS showbuttons,
    axactivemessages.hlink,
    axactivemessages.hlink_transid,
    axactivemessages.hlink_params,
    NULL::text AS taskstatus,
    NULL::text AS statusreason,
    NULL::text AS statustext,
    NULL::text AS cancelremarks,
    NULL::text AS cancelledby,
    NULL::text AS cancelledon,
    NULL::text AS cancel,
    NULL::text AS username,
    'Active'::text AS cstatus
   FROM ({schema}.axactivemessages
     LEFT JOIN {schema}.axpdef_peg_processmaster p ON (((axactivemessages.processname)::text = (p.caption)::text)))
  WHERE (NOT (EXISTS ( SELECT b.taskid
           FROM {schema}.axactivetaskstatus b
          WHERE ((axactivemessages.taskid)::text = (b.taskid)::text))))
UNION ALL
 SELECT a.touser,
    a.processname,
    a.taskname,
    a.taskid,
    a.tasktype,
    a.eventdatetime AS edatetime,
    to_char(to_timestamp((a.eventdatetime)::text, 'YYYYMMDDHH24MISSSSS'::text), 'dd/mm/yyyy hh24:mi:ss'::text) AS eventdatetime,
    a.fromuser,
    a.fromrole,
    a.displayicon,
    a.displaytitle,
    a.displaymcontent,
    a.displaycontent,
    a.displaybuttons,
    a.keyfield,
    a.keyvalue,
    a.transid,
    a.priorindex,
    a.indexno,
    a.subindexno,
    a.approvereasons,
    a.defapptext,
    a.returnreasons,
    a.defrettext,
    a.rejectreasons,
    a.defregtext,
    a.recordid,
    a.approvalcomments,
    a.rejectcomments,
    a.returncomments,
    'PEG'::text AS rectype,
    'NA'::text AS msgtype,
    a.returnable,
    a.initiator,
    a.initiator_approval,
    a.displaysubtitle,
    NULL::character varying AS amendment,
    a.allowsend,
    a.allowsendflg,
    NULL::text AS cmsg_appcheck,
    NULL::text AS cmsg_return,
    NULL::text AS cmsg_reject,
    NULL::character varying AS showbuttons,
    NULL::text AS hlink,
    NULL::text AS hlink_transid,
    NULL::text AS hlink_params,
    {schema}.pr_pegv2_transcurstatus(a.transid, a.keyvalue, a.processname) AS taskstatus,
    b.statusreason,
    b.statustext,
    b.cancelremarks,
    b.cancelledby,
    (b.cancelledon)::character varying AS cancelledon,
    b.cancel,
        CASE
            WHEN (a.indexno = (1)::numeric) THEN a.fromuser
            ELSE a.touser
        END AS username,
    'Completed'::text AS cstatus
   FROM ({schema}.axactivetasks a
     JOIN {schema}.axactivetaskstatus b ON (((a.taskid)::text = (b.taskid)::text)));

CREATE VIEW {schema}.vw_pegv2_completed_tasks AS
 SELECT a.processname,
    a.taskname,
    a.tasktype,
    to_char(to_timestamp((b.eventdatetime)::text, 'YYYYMMDDHH24MISSSSS'::text), 'dd/mm/yyyy hh24:mi:ss'::text) AS eventdatetime,
    a.eventdatetime AS edatetime,
    a.fromuser,
    a.displayicon,
    a.displaytitle,
    a.taskid,
    a.keyfield,
    a.keyvalue,
    a.recordid,
    a.transid,
    a.displaycontent,
    a.indexno,
    b.taskstatus,
    b.statusreason,
    b.statustext,
    b.cancelremarks,
    b.cancelledby,
    b.cancelledon,
    b.cancel,
    b.username
   FROM ({schema}.axactivetasks a
     JOIN {schema}.axactivetaskstatus b ON ((((a.taskid)::text = (b.taskid)::text) AND ((
        CASE
            WHEN (a.indexno = (1)::numeric) THEN a.fromuser
            ELSE a.touser
        END)::text = (b.username)::text))));

CREATE VIEW {schema}.vw_pegv2_global_cards AS
 WITH a AS (
         SELECT a_1.card,
            a_1.ctype,
            a_1.axprocessdefv2id,
            a_1.processname,
            a_1.taskname
           FROM ( SELECT "substring"(unnest(string_to_array((a_2.cards)::text, ','::text)), ("position"(unnest(string_to_array((a_2.cards)::text, ','::text)), '-('::text) + 2), abs((("position"(unnest(string_to_array((a_2.cards)::text, ','::text)), '-('::text) + 2) - length("substring"(unnest(string_to_array((a_2.cards)::text, ','::text)), 1, length(unnest(string_to_array((a_2.cards)::text, ','::text)))))))) AS card,
                    split_part(unnest(string_to_array((a_2.cards)::text, ','::text)), '-('::text, 1) AS ctype,
                    b_1.axprocessdefv2id,
                    b_1.processname,
                    b_1.taskname
                   FROM {schema}.axpdef_peg_processmaster a_2,
                    {schema}.axprocessdefv2 b_1
                  WHERE (((a_2.caption)::text = (b_1.processname)::text) AND ((b_1.stransid)::text <> 'pgv2c'::text))) a_1
          WHERE (a_1.ctype = 'Global'::text)
        ), b AS (
         SELECT a_1.hidecard,
            a_1.ctype,
            a_1.axprocessdefv2id,
            a_1.processname,
            a_1.taskname
           FROM ( SELECT "substring"(unnest(string_to_array((b_1.hidecards)::text, ','::text)), ("position"(unnest(string_to_array((b_1.hidecards)::text, ','::text)), '-('::text) + 2), abs((("position"(unnest(string_to_array((b_1.hidecards)::text, ','::text)), '-('::text) + 2) - length("substring"(unnest(string_to_array((b_1.hidecards)::text, ','::text)), 1, length(unnest(string_to_array((b_1.hidecards)::text, ','::text)))))))) AS hidecard,
                    split_part(unnest(string_to_array((b_1.hidecards)::text, ','::text)), '-('::text, 1) AS ctype,
                    b_1.axprocessdefv2id,
                    b_1.processname,
                    b_1.taskname
                   FROM {schema}.axprocessdefv2 b_1
                  WHERE ((b_1.stransid)::text <> 'pgv2c'::text)) a_1
          WHERE (a_1.ctype = 'Global'::text)
        )
 SELECT a.axprocessdefv2id,
    a.processname,
    a.taskname,
    d.cardname,
    d.axp_cardsid AS cardsid,
    d.sql_editor_cardsql AS cardsql,
    d.cardtype,
    d.charttype,
    d.chartjson,
    'col-md-12'::text AS width,
    'Global'::text AS ctype,
    'NA'::text AS accessstring
   FROM ((a
     LEFT JOIN b ON ((((a.processname)::text = (b.processname)::text) AND ((a.taskname)::text = (b.taskname)::text) AND (a.card = b.hidecard))))
     JOIN {schema}.axp_cards d ON ((a.card = (d.cardname)::text)))
  WHERE (b.hidecard IS NULL);

CREATE VIEW {schema}.vw_pegv2_process_cards AS
 WITH a AS (
         SELECT a_1.card,
            a_1.ctype,
            a_1.axprocessdefv2id,
            a_1.processname,
            a_1.taskname
           FROM ( SELECT "substring"(unnest(string_to_array((a_2.cards)::text, ','::text)), ("position"(unnest(string_to_array((a_2.cards)::text, ','::text)), '-('::text) + 2), abs((("position"(unnest(string_to_array((a_2.cards)::text, ','::text)), '-('::text) + 2) - length("substring"(unnest(string_to_array((a_2.cards)::text, ','::text)), 1, length(unnest(string_to_array((a_2.cards)::text, ','::text)))))))) AS card,
                    split_part(unnest(string_to_array((a_2.cards)::text, ','::text)), '-('::text, 1) AS ctype,
                    b_1.axprocessdefv2id,
                    b_1.processname,
                    b_1.taskname
                   FROM {schema}.axpdef_peg_processmaster a_2,
                    {schema}.axprocessdefv2 b_1
                  WHERE (((a_2.caption)::text = (b_1.processname)::text) AND ((b_1.stransid)::text <> 'pgv2c'::text))) a_1
          WHERE (a_1.ctype = 'Process'::text)
        ), b AS (
         SELECT a_1.hidecard,
            a_1.ctype,
            a_1.axprocessdefv2id,
            a_1.processname,
            a_1.taskname
           FROM ( SELECT "substring"(unnest(string_to_array((b_1.hidecards)::text, ','::text)), ("position"(unnest(string_to_array((b_1.hidecards)::text, ','::text)), '-('::text) + 2), abs((("position"(unnest(string_to_array((b_1.hidecards)::text, ','::text)), '-('::text) + 2) - length("substring"(unnest(string_to_array((b_1.hidecards)::text, ','::text)), 1, length(unnest(string_to_array((b_1.hidecards)::text, ','::text)))))))) AS hidecard,
                    split_part(unnest(string_to_array((b_1.hidecards)::text, ','::text)), '-('::text, 1) AS ctype,
                    b_1.axprocessdefv2id,
                    b_1.processname,
                    b_1.taskname
                   FROM {schema}.axprocessdefv2 b_1
                  WHERE ((b_1.stransid)::text <> 'pgv2c'::text)) a_1
          WHERE (a_1.ctype = 'Process'::text)
        )
 SELECT a.axprocessdefv2id,
    a.processname,
    a.taskname,
    d.cardname,
    d.axpdef_prcardsid AS cardsid,
    d.sql_editor_cardsql AS cardsql,
    d.cardtype,
    d.charttype,
    d.chartjson,
    'col-md-12'::text AS width,
    'Process'::text AS ctype,
    'NA'::text AS accessstring
   FROM ((a
     LEFT JOIN b ON ((((a.processname)::text = (b.processname)::text) AND ((a.taskname)::text = (b.taskname)::text) AND (a.card = b.hidecard))))
     JOIN {schema}.axpdef_prcards d ON ((a.card = (d.cardname)::text)))
  WHERE (b.hidecard IS NULL);

CREATE VIEW {schema}.vw_pegv2_processdef_cards AS
 WITH a AS (
         SELECT a_1.caption,
            c.taskname,
            regexp_split_to_table((a_1.cards)::text, ','::text) AS card,
            c.axprocessdefv2id
           FROM {schema}.axpdef_peg_processmaster a_1,
            {schema}.axprocessdefv2 c
          WHERE (a_1.axpdef_peg_processmasterid = c.axpdef_peg_processmasterid)
        ), b AS (
         SELECT axprocessdefv2.processname,
            axprocessdefv2.taskname,
            regexp_split_to_table((COALESCE(axprocessdefv2.hidecards, 'NA'::character varying))::text, ','::text) AS hidecard
           FROM {schema}.axprocessdefv2
        )
 SELECT a.axprocessdefv2id,
    a.caption AS processname,
    a.taskname,
    d.cardname,
    d.axp_cardsid,
    d.sql_editor_cardsql,
    d.cardtype,
    d.chartjson,
    d.pagecaption,
    d.pagename,
    d.hcaption,
    d.htype,
    d.htransid
   FROM ((a
     LEFT JOIN b ON ((((a.caption)::text = (b.processname)::text) AND ((a.taskname)::text = (b.taskname)::text) AND (a.card = b.hidecard))))
     JOIN {schema}.axp_cards d ON ((a.card = (d.cardname)::text)))
  WHERE (b.hidecard IS NULL);

CREATE VIEW {schema}.vw_pegv2_processdef_tree AS
 SELECT processname,
    taskname,
    tasktype,
    taskgroupname AS taskgroup,
    active AS taskactive,
    indexno,
    subindexno AS subindex,
    groupwithprior AS details,
    transid,
    axprocessdefv2id AS recordid,
    displayicon,
    groupwithprior,
    keyfield
   FROM {schema}.axprocessdefv2;

CREATE VIEW {schema}.vw_purchase_header AS
 SELECT a.company,
    a.purchase_bill_headerid,
    a.docid,
    a.docdate,
    a.transid,
    b.branchname,
    c.party_name AS supplier,
    a.total_billvalue,
    a.total_itemvalue,
    a.total_taxamount,
    (0)::numeric AS tothercostvalue,
    c.mg_supplierid
   FROM (({schema}.purchase_bill_header a
     JOIN {schema}.branch b ON ((a.branch = b.branchid)))
     LEFT JOIN {schema}.mg_supplier c ON ((c.mg_supplierid = a.supplier)))
  WHERE (((a.cancel)::text = 'F'::text) AND (a.app_desc = (1)::numeric))
UNION
 SELECT d.company,
    d.purchasereturn_headerid AS purchase_bill_headerid,
    d.docid,
    d.docdate,
    d.transid,
    b.branchname,
    c.party_name AS supplier,
    (- d.total_billvalue) AS total_billvalue,
    (- d.total_itemvalue) AS total_itemvalue,
    (- d.total_taxamount) AS total_taxamount,
    (- d.tothercostvalue) AS tothercostvalue,
    c.mg_supplierid
   FROM (({schema}.purchasereturn_header d
     JOIN {schema}.branch b ON ((d.branch = b.branchid)))
     JOIN {schema}.mg_supplier c ON ((c.mg_supplierid = d.supplier)))
  WHERE (((d.cancel)::text = 'F'::text) AND (d.app_desc = (1)::numeric))
  ORDER BY 3;

CREATE VIEW {schema}.vw_purchase_items AS
 SELECT a.company,
    a.purchase_bill_headerid AS headerid,
    a.docid,
    a.docdate,
    a.transid,
    b.branchname,
    c.party_name AS supplier,
    a.total_billvalue,
    a.total_itemvalue,
    a.total_taxamount,
    (0)::numeric AS tothercostvalue,
    i.itemdesc,
    i.itemid,
    e.qty,
    e.grossamount,
    e.discamount,
    e.netamount,
    e.taxableamount,
    ic.categoryname,
    p.productcategory,
    e.taxcategory,
    bm.brand_name,
    a.currency,
    e.netamtwotax,
    c.mg_supplierid
   FROM ((((((({schema}.purchase_bill_header a
     JOIN {schema}.purchase_bill_items e ON ((a.purchase_bill_headerid = e.purchase_bill_headerid)))
     JOIN {schema}.branch b ON ((a.branch = b.branchid)))
     LEFT JOIN {schema}.mg_supplier c ON ((c.mg_supplierid = a.supplier)))
     JOIN {schema}.item i ON ((i.itemid = e.itemname)))
     JOIN {schema}.itemcategory ic ON ((ic.itemcategoryid = i.itemcategory)))
     JOIN {schema}.productcategory p ON ((p.productcategoryid = i.productcategory)))
     LEFT JOIN {schema}.brand_master bm ON ((bm.brand_masterid = i.itembrand)))
  WHERE (((a.cancel)::text = 'F'::text) AND (a.app_desc = (1)::numeric))
UNION
 SELECT d.company,
    d.purchasereturn_headerid AS headerid,
    d.docid,
    d.docdate,
    d.transid,
    b.branchname,
    c.party_name AS supplier,
    (- d.total_billvalue) AS total_billvalue,
    (- d.total_itemvalue) AS total_itemvalue,
    (- d.total_taxamount) AS total_taxamount,
    (- d.tothercostvalue) AS tothercostvalue,
    i.itemdesc,
    i.itemid,
    (- f.returnqty) AS qty,
    (- f.grossamount) AS grossamount,
    (- f.discamount) AS discamount,
    (- f.netamount) AS netamount,
    (- f.taxableamount) AS taxableamount,
    ic.categoryname,
    p.productcategory,
    f.taxcategory,
    bm.brand_name,
    cr.currency,
    f.netamtwotax,
    c.mg_supplierid
   FROM (((((((({schema}.purchasereturn_header d
     JOIN {schema}.purchasereturn_items f ON ((d.purchasereturn_headerid = f.purchasereturn_headerid)))
     JOIN {schema}.branch b ON ((d.branch = b.branchid)))
     JOIN {schema}.mg_supplier c ON ((c.mg_supplierid = d.supplier)))
     JOIN {schema}.item i ON ((i.itemid = f.itemname)))
     JOIN {schema}.itemcategory ic ON ((ic.itemcategoryid = i.itemcategory)))
     JOIN {schema}.productcategory p ON ((p.productcategoryid = i.productcategory)))
     LEFT JOIN {schema}.brand_master bm ON ((bm.brand_masterid = i.itembrand)))
     LEFT JOIN {schema}.currency cr ON ((cr.currencyid = d.currency)))
  WHERE (((d.cancel)::text = 'F'::text) AND (d.app_desc = (1)::numeric))
  ORDER BY 3;

CREATE VIEW {schema}.vw_purchasegst_details AS
 SELECT a.company,
    b.branchname,
    a.branch,
    a.docid,
    a.docdate,
    m.party_name,
    m.tinno AS gstregno,
        CASE
            WHEN ((m.gstregistered)::text = 'T'::text) THEN 'T'::text
            ELSE 'F'::text
        END AS gstregistered,
    a.transid,
    a.supply_type,
    0 AS cess_amount,
    i.itemdesc,
    i.itemcode,
    h.code,
    h.description,
    i.uom,
    c.qty,
    c.rate,
    c.netamount AS net_amount,
    c.sgstamount,
    c.cgstamount,
    c.igstamount,
    'Debit'::text AS transaction_type,
    (c.grossamount + COALESCE(c.discamount, (0)::numeric)) AS gross_amount,
    a.purchase_bill_headerid AS headerid,
    c.taxcategory,
    g.docid AS returnid,
    c.taxableamount,
    c.purchase_bill_itemsrow AS rowno,
    NULL::text AS batch,
    m.mg_supplierid
   FROM ((((((({schema}.purchase_bill_header a
     JOIN {schema}.accountshdr a2 ON ((a.purchase_bill_headerid = a2.sourceid)))
     JOIN {schema}.branch b ON ((b.branchid = a.branch)))
     JOIN {schema}.mg_supplier m ON ((m.mg_supplierid = a.supplier)))
     JOIN {schema}.purchase_bill_items c ON ((c.purchase_bill_headerid = a.purchase_bill_headerid)))
     JOIN {schema}.item i ON ((i.itemid = c.itemname)))
     JOIN {schema}.hsnsac_codes h ON ((h.hsnsac_codesid = i.hsnno)))
     JOIN {schema}.grn_header g ON ((g.grn_headerid = c.gdocid)))
  WHERE ((a.cancel)::text = 'F'::text)
UNION
 SELECT a.company,
    b.branchname,
    a.branch,
    a.purchasebillno AS docid,
    a.docdate,
    m.party_name,
    m.tinno AS gstregno,
        CASE
            WHEN ((m.gstregistered)::text = 'T'::text) THEN 'T'::text
            ELSE 'F'::text
        END AS gstregistered,
    a.transid,
    a.supply_type,
    0 AS cess_amount,
    i.itemdesc,
    i.itemcode,
    h.code,
    h.description,
    i.uom,
    (- c.returnqty) AS qty,
    c.rate,
    (- c.netamount) AS net_amount,
    (- c.sgstamount) AS sgstamount,
    (- c.cgstamount) AS cgstamount,
    (- c.igstamount) AS igstamount,
    'Credit'::text AS transaction_type,
    (- ((c.grossamount + COALESCE(c.discamount, (0)::numeric)) + COALESCE(c.othercharges, (0)::numeric))) AS gross_amount,
    a.purchasereturn_headerid AS headerid,
    c.taxcategory,
    a.docid AS returnid,
    c.taxableamount,
    c.purchasereturn_itemsrow AS rowno,
    split_part((c.batchserialbreakup)::text, '|'::text, 1) AS batch,
    m.mg_supplierid
   FROM (((((({schema}.purchasereturn_header a
     LEFT JOIN {schema}.accountshdr a2 ON ((a.purchasereturn_headerid = a2.sourceid)))
     JOIN {schema}.branch b ON ((b.branchid = a.branch)))
     JOIN {schema}.mg_supplier m ON ((m.mg_supplierid = a.supplier)))
     JOIN {schema}.purchasereturn_items c ON ((c.purchasereturn_headerid = a.purchasereturn_headerid)))
     JOIN {schema}.item i ON ((i.itemid = c.itemname)))
     JOIN {schema}.hsnsac_codes h ON ((h.hsnsac_codesid = i.hsnno)))
  WHERE ((a.cancel)::text = 'F'::text)
  ORDER BY 4;

CREATE VIEW {schema}.vw_salesgst_details AS
 SELECT a.company,
    b.branchname,
    a.branch,
    a.docid,
    a.docdate,
    m.party_name,
    a.state AS place_of_supply,
    m.tinno AS gstregno,
        CASE
            WHEN ((m.gstregistered)::text = 'T'::text) THEN 'T'::text
            ELSE 'F'::text
        END AS gstregistered,
    a.transid,
    a.supply_type,
    0 AS cess_amount,
    i.itemdesc,
    i.itemcode,
    h.code,
    h.description,
    i.uom,
    c.invoice_qty AS qty,
    c.rate,
    c.net_amount,
    c.sgstamount,
    c.cgstamount,
    c.igstamount,
    'Credit'::text AS transaction_type,
    c.gross_afterdiscount,
    a.invoiceheaderid AS headerid,
    c.taxcategory,
        CASE
            WHEN ((b.tinno)::text = 'T'::text) THEN 'T'::text
            ELSE 'F'::text
        END AS gstenabled,
    NULL::character varying AS returnid,
    i.hsntaxrate,
    c.taxableamount,
    c.discount,
    c.disper,
    c.gross_amount,
    0 AS printinvoice,
    c.invoiceitemsrow AS rowno,
    c.salesorder_qty AS orderqty,
    ''::text AS batch
   FROM (((((({schema}.invoiceheader a
     JOIN {schema}.invoiceitems c ON ((c.invoiceheaderid = a.invoiceheaderid)))
     JOIN {schema}.accountshdr a2 ON ((a.invoiceheaderid = a2.sourceid)))
     JOIN {schema}.item i ON ((i.itemid = c.itemname)))
     JOIN {schema}.branch b ON ((b.branchid = a.branch)))
     JOIN {schema}.hsnsac_codes h ON ((h.hsnsac_codesid = i.hsnno)))
     LEFT JOIN {schema}.mg_customer m ON ((m.mg_customerid = a.customer)))
  WHERE (((a.cancel)::text = 'F'::text) AND (a.app_desc = (1)::numeric))
UNION
 SELECT a.company,
    b.branchname,
    a.branch,
    s.docid,
    a.docdate,
    m.party_name,
    a.state AS place_of_supply,
    m.tinno AS gstregno,
        CASE
            WHEN ((m.gstregistered)::text = 'T'::text) THEN 'T'::text
            ELSE 'F'::text
        END AS gstregistered,
    a.transid,
    a.supply_type,
    0 AS cess_amount,
    i.itemdesc,
    i.itemcode,
    h.code,
    h.description,
    i.uom,
    (- c.salesreturn_qty) AS qty,
    c.rate,
    (- c.net_amount) AS net_amount,
    (- c.sgstamount) AS sgstamount,
    (- c.cgstamount) AS cgstamount,
    (- c.igstamount) AS igstamount,
    'Debit'::text AS transaction_type,
    (- c.gross_afterdiscount) AS gross_afterdiscount,
    a.salesreturns_headerid AS headerid,
    c.taxcategory,
        CASE
            WHEN ((b.tinno)::text = 'T'::text) THEN 'T'::text
            ELSE 'F'::text
        END AS gstenabled,
    a.docid AS returnid,
    i.hsntaxrate,
    c.taxableamount,
    c.discount,
    c.disper,
    c.gross_amount,
    0 AS printinvoice,
    c.salesreturns_itemsrow AS rowno,
    si.invoice_qty AS orderqty,
    ''::text AS batch
   FROM (((((((({schema}.salesreturns_header a
     JOIN {schema}.salesreturns_items c ON ((a.salesreturns_headerid = c.salesreturns_headerid)))
     JOIN {schema}.accountshdr a2 ON ((a.salesreturns_headerid = a2.sourceid)))
     JOIN {schema}.item i ON ((i.itemid = c.itemname)))
     JOIN {schema}.branch b ON ((b.branchid = a.branch)))
     JOIN {schema}.hsnsac_codes h ON ((h.hsnsac_codesid = i.hsnno)))
     JOIN {schema}.mg_customer m ON ((m.mg_customerid = a.customer)))
     LEFT JOIN {schema}.invoiceheader s ON (((s.docid)::text = (c.msalesinvoice_number)::text)))
     LEFT JOIN {schema}.invoiceitems si ON (((si.invoiceheaderid = s.invoiceheaderid) AND (si.itemname = c.itemname))))
  WHERE (((a.cancel)::text = 'F'::text) AND (a.app_desc = (1)::numeric))
UNION
 SELECT a.company,
    b.branchname,
    a.branch,
    s.docid,
    a.docdate,
    m.party_name,
    a.state AS place_of_supply,
    m.tinno AS gstregno,
        CASE
            WHEN ((m.gstregistered)::text = 'T'::text) THEN 'T'::text
            ELSE 'F'::text
        END AS gstregistered,
    a.transid,
    a.supply_type,
    0 AS cess_amount,
    i.itemdesc,
    i.itemcode,
    h.code,
    h.description,
    i.uom,
    (- c.salesreturn_qty) AS qty,
    c.rate,
    (- c.net_amount) AS net_amount,
    (- c.sgstamount) AS sgstamount,
    (- c.cgstamount) AS cgstamount,
    (- c.igstamount) AS igstamount,
    'Debit'::text AS transaction_type,
    (- c.gross_afterdiscount) AS gross_afterdiscount,
    a.salesreturns_headerid AS headerid,
    c.taxcategory,
        CASE
            WHEN ((b.tinno)::text = 'T'::text) THEN 'T'::text
            ELSE 'F'::text
        END AS gstenabled,
    a.docid AS returnid,
    i.hsntaxrate,
    c.taxableamount,
    c.discount,
    c.disper,
    c.gross_amount,
    0 AS printinvoice,
    c.salesreturns_itemsrow AS rowno,
    si.invoice_qty AS orderqty,
    ''::text AS batch
   FROM ((((((({schema}.salesreturns_header a
     JOIN {schema}.salesreturns_items c ON ((a.salesreturns_headerid = c.salesreturns_headerid)))
     JOIN {schema}.item i ON ((i.itemid = c.itemname)))
     JOIN {schema}.branch b ON ((b.branchid = a.branch)))
     JOIN {schema}.hsnsac_codes h ON ((h.hsnsac_codesid = i.hsnno)))
     JOIN {schema}.mg_customer m ON ((m.mg_customerid = a.customer)))
     LEFT JOIN {schema}.invoiceheader s ON (((s.docid)::text = (c.msalesinvoice_number)::text)))
     LEFT JOIN {schema}.invoiceitems si ON (((si.invoiceheaderid = s.invoiceheaderid) AND (si.itemname = c.itemname))))
  WHERE (((a.cancel)::text = 'F'::text) AND (a.app_desc = (1)::numeric))
  ORDER BY 4;

CREATE VIEW {schema}.vw_salesgst_summary AS
 SELECT a.company,
    b.branchname,
    a.docid,
    a.docdate,
    m.party_name,
    a.state AS place_of_supply,
    m.tinno AS gstregno,
        CASE
            WHEN ((m.gstregistered)::text = 'T'::text) THEN 'T'::text
            ELSE 'F'::text
        END AS gstregistered,
    a.transid,
    a.supply_type,
    a.total_sales_amount AS total_net_amount,
    a.total_grossamount AS total_gross_amount,
    a.total_sgstamount,
    a.total_cgstamount,
    a.total_igstamount,
    0 AS cess_amount,
    'Credit'::text AS transaction_type,
    a.invoiceheaderid AS headerid,
    'F'::text AS gstenabled,
    NULL::character varying AS returnid,
    m.mobileno,
    l.locationname,
    ''::text AS modeofpayment,
    0 AS printinvoice,
    a.channel,
    m.currency,
    COALESCE((((((((m.primary_address)::text || ', '::text) || (m.city)::text) || ', '::text) || (m.statejurisdiction)::text) || ' - '::text) || (m.pincode)::text), ''::text) AS cust_address,
    a.sales_person,
    ''::text AS onaccountof
   FROM (((({schema}.invoiceheader a
     JOIN {schema}.accountshdr a2 ON ((a.invoiceheaderid = a2.sourceid)))
     JOIN {schema}.branch b ON ((b.branchid = a.branch)))
     JOIN {schema}.location l ON ((a.location = l.locationid)))
     LEFT JOIN {schema}.mg_customer m ON ((m.mg_customerid = a.customer)))
  WHERE (((a.cancel)::text = 'F'::text) AND (a.app_desc = (1)::numeric))
UNION
 SELECT a.company,
    b.branchname,
    ''::character varying AS docid,
    a.docdate,
    m.party_name,
    a.state AS place_of_supply,
    m.tinno AS gstregno,
        CASE
            WHEN ((m.gstregistered)::text = 'T'::text) THEN 'T'::text
            ELSE 'F'::text
        END AS gstregistered,
    a.transid,
    a.supply_type,
    (- a.total_sales_amount) AS total_net_amount,
    (- a.total_grossamount) AS total_gross_amount,
    (- a.total_sgstamount) AS total_sgstamount,
    (- a.total_cgstamount) AS total_cgstamount,
    (- a.total_igstamount) AS total_igstamount,
    0 AS cess_amount,
    'Debit'::text AS transaction_type,
    a.salesreturns_headerid AS headerid,
    'F'::text AS gstenabled,
    a.docid AS returnid,
    m.mobileno,
    l.locationname,
    ''::text AS modeofpayment,
    0 AS printinvoice,
    a.channel,
    m.currency,
    COALESCE((((((((m.primary_address)::text || ', '::text) || (m.city)::text) || ', '::text) || (m.statejurisdiction)::text) || ' - '::text) || (m.pincode)::text), ''::text) AS cust_address,
    m.salesman AS sales_person,
    ''::text AS onaccountof
   FROM (((({schema}.salesreturns_header a
     JOIN {schema}.accountshdr a2 ON ((a.salesreturns_headerid = a2.sourceid)))
     JOIN {schema}.branch b ON ((b.branchid = a.branch)))
     JOIN {schema}.location l ON ((a.location = l.locationid)))
     JOIN {schema}.mg_customer m ON ((m.mg_customerid = a.customer)))
  WHERE (((a.cancel)::text = 'F'::text) AND (a.app_desc = (1)::numeric))
UNION
 SELECT DISTINCT ON (a.salesreturns_headerid) a.company,
    b.branchname,
    s.docid,
    a.docdate,
    m.party_name,
    a.state AS place_of_supply,
    m.tinno AS gstregno,
        CASE
            WHEN ((m.gstregistered)::text = 'T'::text) THEN 'T'::text
            ELSE 'F'::text
        END AS gstregistered,
    a.transid,
    a.supply_type,
    (- a.total_sales_amount) AS total_net_amount,
    (- a.total_grossamount) AS total_gross_amount,
    (- a.total_sgstamount) AS total_sgstamount,
    (- a.total_cgstamount) AS total_cgstamount,
    (- a.total_igstamount) AS total_igstamount,
    0 AS cess_amount,
    'Debit'::text AS transaction_type,
    a.salesreturns_headerid AS headerid,
    'F'::text AS gstenabled,
    a.docid AS returnid,
    m.mobileno,
    l.locationname,
    ''::text AS modeofpayment,
    0 AS printinvoice,
    a.channel,
    m.currency,
    COALESCE((((((((m.primary_address)::text || ', '::text) || (m.city)::text) || ', '::text) || (m.statejurisdiction)::text) || ' - '::text) || (m.pincode)::text), ''::text) AS cust_address,
    s.sales_person,
    ''::text AS onaccountof
   FROM ((((({schema}.salesreturns_header a
     JOIN {schema}.salesreturns_items d ON ((a.salesreturns_headerid = d.salesreturns_headerid)))
     LEFT JOIN {schema}.invoiceheader s ON (((s.docid)::text = (d.msalesinvoice_number)::text)))
     JOIN {schema}.branch b ON ((b.branchid = a.branch)))
     JOIN {schema}.location l ON ((a.location = l.locationid)))
     JOIN {schema}.mg_customer m ON ((m.mg_customerid = a.customer)))
  WHERE (((a.cancel)::text = 'F'::text) AND (a.app_desc = (1)::numeric))
  ORDER BY 3;

CREATE VIEW {schema}.vw_salesorder AS
 SELECT a.docid,
    b.party_name,
    b.statejurisdiction AS state,
    a.currency,
    a.barqr_code,
    ''::text AS modeofpayment,
    ''::text AS onaccountof,
    a.channel,
    0 AS shipping_charges,
    a.docdate,
    b.tinno,
    a.sales_person,
    {schema}.f_amount_to_words(a.total_netamount, (d.dispcurrency)::text) AS netamount,
    to_char(CURRENT_TIMESTAMP, 'DD/MM/YYYY HH24:MI:SS'::text) AS system_datetime,
    a.total_taxamount,
    a.total_grossamount,
    a.total_netamount,
    (a.total_netamount + (0)::numeric) AS total_amount
   FROM (({schema}.salesorder_header a
     JOIN {schema}.mg_customer b ON ((b.mg_customerid = a.customer)))
     JOIN {schema}.company d ON ((d.companyid = a.company)))
  GROUP BY a.docid, b.party_name, b.statejurisdiction, a.currency, a.barqr_code, a.channel, a.total_taxamount, a.docdate, b.tinno, a.sales_person, a.total_netamount, a.total_grossamount, a.total_discount, d.dispcurrency;

CREATE VIEW {schema}.vw_taxratedtl AS
 SELECT hsnsac_codesid AS taxratedtlid,
    company,
    hsnsac_codesid AS taxrateid,
    to_date('01-jan-1990'::text, 'dd-mon-yyyy'::text) AS effectivefrom,
    rate AS taxrate,
    0 AS cesstaxrate,
    rate AS srcmtax,
    rate AS prcmtax,
    1 AS rnk
   FROM {schema}.hsnsac_codes a;

CREATE VIEW {schema}.vw_userbranchlocations AS
 SELECT a.username,
    a.usertype,
    co.companyname,
    co.shortname AS companycode,
    br.branchname,
    br.branchid,
    br.branchid AS vw_userbranchlocationsid,
    co.companyid,
    br.bridentifier,
    br.state,
    br.tinno,
    co.gstenabled,
        CASE
            WHEN (upper((br.state)::text) = upper((co.state)::text)) THEN 'Intra State'::text
            ELSE 'Inter State'::text
        END AS supplytype,
    t.cgstinputtax,
    t.cgstoutputtax,
    t.sgstinputtax,
    t.sgstoutputtax,
    t.igstinputtax,
    t.igstoutputtax
   FROM {schema}.axusers a,
    ( SELECT b_1.axusersid,
            unnest(string_to_array((b_1.axug_branch)::text, ','::text)) AS branchname
           FROM {schema}.axusergrouping b_1) b,
    {schema}.company co,
    {schema}.branch br,
    {schema}.taxtypes t
  WHERE ((a.axusersid = b.axusersid) AND (br.companyname = co.companyid) AND ((br.cancel)::text = 'F'::text) AND ((COALESCE(br.active, 'F'::character varying))::text = 'T'::text) AND (b.branchname = (br.branchname)::text) AND (lower((a.allusergroup)::text) !~~ '%default%'::text) AND ((co.taxtype)::text = (t.taxcode)::text))
UNION ALL
 SELECT a.username,
    a.usertype,
    co.companyname,
    co.shortname AS companycode,
    br.branchname,
    br.branchid,
    br.branchid AS vw_userbranchlocationsid,
    co.companyid,
    br.bridentifier,
    br.state,
    br.tinno,
    co.gstenabled,
        CASE
            WHEN (upper((br.state)::text) = upper((co.state)::text)) THEN 'Intra State'::text
            ELSE 'Inter State'::text
        END AS supplytype,
    t.cgstinputtax,
    t.cgstoutputtax,
    t.sgstinputtax,
    t.sgstoutputtax,
    t.igstinputtax,
    t.igstoutputtax
   FROM {schema}.axusers a,
    {schema}.company co,
    {schema}.branch br,
    {schema}.taxtypes t
  WHERE ((br.companyname = co.companyid) AND ((br.cancel)::text = 'F'::text) AND ((COALESCE(br.active, 'F'::character varying))::text = 'T'::text) AND (lower((a.allusergroup)::text) ~~ '%default%'::text) AND ((co.taxtype)::text = (t.taxcode)::text))
  ORDER BY 1;

CREATE VIEW {schema}.vw_userbranchlocations_bkp_25052026 AS
 SELECT a.username,
    a.usertype,
    co.companyname,
    co.shortname AS companycode,
    br.branchname,
    br.branchid,
    br.branchid AS vw_userbranchlocationsid,
    co.companyid,
    br.bridentifier,
    br.state,
    br.tinno,
    co.gstenabled,
        CASE
            WHEN (upper((br.state)::text) = upper((co.state)::text)) THEN 'Intra State'::text
            ELSE 'Inter State'::text
        END AS supplytype,
    t.sgstoutputtax,
    t.cgstoutputtax,
    t.igstoutputtax,
    t.sgstinputtax,
    t.cgstinputtax,
    t.igstinputtax
   FROM {schema}.axusers a,
    {schema}.axuserbranch b,
    {schema}.company co,
    {schema}.branch br,
    {schema}.axglovar x,
    {schema}.taxtypes t
  WHERE ((a.axusersid = b.axusersid) AND (b.companyid = co.companyid) AND ((br.cancel)::text = 'F'::text) AND ((COALESCE(br.active, 'F'::character varying))::text = 'T'::text) AND (b.branchid = br.branchid) AND ((a.username)::text = (x.axglo_user)::text) AND ((a.accesstobranches)::text = 'F'::text) AND (x.m_companyid = co.companyid) AND ((co.taxtype)::text = (t.taxcode)::text) AND (co.companyid = t.company))
UNION ALL
 SELECT a.username,
    a.usertype,
    co.companyname,
    co.shortname AS companycode,
    br.branchname,
    br.branchid,
    br.branchid AS vw_userbranchlocationsid,
    co.companyid,
    br.bridentifier,
    br.state,
    br.tinno,
    co.gstenabled,
        CASE
            WHEN (upper((br.state)::text) = upper((co.state)::text)) THEN 'Intra State'::text
            ELSE 'Inter State'::text
        END AS supplytype,
    t.sgstoutputtax,
    t.cgstoutputtax,
    t.igstoutputtax,
    t.sgstinputtax,
    t.cgstinputtax,
    t.igstinputtax
   FROM {schema}.axusers a,
    {schema}.company co,
    {schema}.branch br,
    {schema}.axglovar x,
    {schema}.taxtypes t
  WHERE (((a.username)::text = (x.axglo_user)::text) AND ((a.accesstobranches)::text = 'T'::text) AND (x.m_companyid = co.companyid) AND (br.companyname = co.companyid) AND ((br.cancel)::text = 'F'::text) AND ((COALESCE(br.active, 'F'::character varying))::text = 'T'::text) AND ((co.taxtype)::text = (t.taxcode)::text) AND (co.companyid = t.company));

CREATE VIEW {schema}.vw_userbranchlocations_global AS
 SELECT a.username,
    a.usertype,
    co.companyname,
    co.shortname AS companycode,
    br.branchname,
    br.branchid,
    co.companyid,
    br.bridentifier,
    br.state,
    br.tinno,
    co.gstenabled
   FROM {schema}.axusers a,
    {schema}.axuserbranch b,
    {schema}.company co,
    {schema}.branch br
  WHERE ((a.axusersid = b.axusersid) AND (b.companyid = co.companyid) AND ((br.cancel)::text = 'F'::text) AND ((COALESCE(br.active, 'F'::character varying))::text = 'T'::text) AND (b.branchid = br.branchid))
UNION
 SELECT a.username,
    a.usertype,
    co.companyname,
    co.shortname AS companycode,
    br.branchname,
    br.branchid,
    co.companyid,
    br.bridentifier,
    br.state,
    br.tinno,
    co.gstenabled
   FROM {schema}.axusers a,
    {schema}.company co,
    {schema}.branch br,
    ( SELECT DISTINCT axuserlevelgroups.username
           FROM {schema}.axuserlevelgroups
          WHERE (lower((axuserlevelgroups.usergroup)::text) = 'default'::text)) u
  WHERE ((br.companyname = co.companyid) AND ((br.cancel)::text = 'F'::text) AND ((br.active)::text = 'T'::text) AND ((a.username)::text = (u.username)::text));

CREATE VIEW {schema}.vw_userloactions AS
 SELECT l.locationid,
    l.locationname,
    l.branch,
    l.companyname,
    l.isgit,
    ''::text AS username,
    b.branchname
   FROM {schema}.location l,
    {schema}.branch b
  WHERE ((l.branch = b.branchid) AND ((l.active)::text = 'T'::text) AND ((l.cancel)::text = 'F'::text) AND ((b.active)::text = 'T'::text));

CREATE VIEW {schema}.vw_username_role_menu_access AS
 SELECT a2.username,
    a3.groupname,
    a5.rname,
    a5.sname,
    a5.stype,
        CASE a5.stype
            WHEN 't'::text THEN t.caption
            WHEN 'i'::text THEN i.caption
            WHEN 'p'::text THEN p.caption
            ELSE NULL::character varying
        END AS caption
   FROM (((((({schema}.axusergroups a3
     JOIN {schema}.axusergroupsdetail a4 ON ((a3.axusergroupsid = a4.axusergroupsid)))
     JOIN {schema}.axuseraccess a5 ON (((a4.roles_id)::text = (a5.rname)::text)))
     LEFT JOIN {schema}.axuserlevelgroups a2 ON ((((a2.usergroup)::text = (a3.groupname)::text) AND ((a2.usergroup)::text <> 'default'::text))))
     LEFT JOIN {schema}.tstructs t ON ((((a5.sname)::text = (t.name)::text) AND (t.blobno = (1)::numeric))))
     LEFT JOIN {schema}.iviews i ON (((a5.sname)::text = (i.name)::text)))
     LEFT JOIN {schema}.axpages p ON ((((a5.sname)::text = (p.name)::text) AND ((p.pagetype)::text = 'web'::text))))
UNION ALL
 SELECT DISTINCT a2.username,
    'default'::text AS groupname,
    'default'::text AS rname,
    t.name AS sname,
    't'::text AS stype,
    t.caption
   FROM ({schema}.tstructs t
     LEFT JOIN {schema}.axuserlevelgroups a2 ON (((a2.usergroup)::text = 'default'::text)))
  WHERE (t.blobno = (1)::numeric)
UNION ALL
 SELECT DISTINCT a2.username,
    'default'::text AS groupname,
    'default'::text AS rname,
    i.name AS sname,
    'i'::text AS stype,
    i.caption
   FROM ({schema}.iviews i
     LEFT JOIN {schema}.axuserlevelgroups a2 ON (((a2.usergroup)::text = 'default'::text)))
UNION ALL
 SELECT DISTINCT a2.username,
    'default'::text AS groupname,
    'default'::text AS rname,
    p.name AS sname,
    'p'::text AS stype,
    p.caption
   FROM ({schema}.axpages p
     LEFT JOIN {schema}.axuserlevelgroups a2 ON (((a2.usergroup)::text = 'default'::text)))
  WHERE ((p.pagetype)::text = 'web'::text);
