-- FK dependency ordered full seed data.
-- Parent/reference tables are inserted before dependent child tables based on FK constraints.
SET LOCAL search_path = {schema}, pg_catalog;

-- Seed: axpexception (0 rows)
-- No rows in source dump for axpexception.

-- Seed: axpexchange (0 rows)
-- No rows in source dump for axpexchange.

-- Seed: axpfgdtl (97 rows)
INSERT INTO {schema}.axpfgdtl (tstruct, fgname, srcfld, tarfld, caption)
VALUES
('b_sql', 'fillgrid1', 'fldname', 'fldname', 'Field name'),
('b_sql', 'fillgrid1', 'fldcaption', 'fldcaption', 'Caption'),
('b_sql', 'fillgrid1', 'datatypeui', 'datatypeui', 'Datatype'),
('b_sql', 'fillgrid1', 'filter', 'filter', 'Filter'),
('b_sql', 'fillgrid1', 'sourcetable', 'sourcetable', 'Source table'),
('b_sql', 'fillgrid1', 'sourcefld', 'sourcefld', 'Source field'),
('b_sql', 'fillgrid1', 'hyp_structtype', 'hyp_structtype', 'Hyperlink type'),
('b_sql', 'fillgrid1', 'hyp_struct', 'hyp_struct', 'Hyperlink target'),
('b_sql', 'fillgrid1', 'tbl_hyperlink', 'tbl_hyperlink', 'Hyperlink mappings'),
('b_sql', 'fillgrid1', 'fldname', 'fldname', 'Field name'),
('b_sql', 'fillgrid1', 'fldcaption', 'fldcaption', 'Caption'),
('b_sql', 'fillgrid1', 'datatypeui', 'datatypeui', 'Datatype'),
('b_sql', 'fillgrid1', 'filter', 'filter', 'Filter'),
('b_sql', 'fillgrid1', 'sourcetable', 'sourcetable', 'Source table'),
('b_sql', 'fillgrid1', 'sourcefld', 'sourcefld', 'Source field'),
('b_sql', 'fillgrid1', 'hyp_structtype', 'hyp_structtype', 'Hyperlink type'),
('b_sql', 'fillgrid1', 'hyp_struct', 'hyp_struct', 'Hyperlink target'),
('b_sql', 'fillgrid1', 'tbl_hyperlink', 'tbl_hyperlink', 'Hyperlink mappings'),
('a__sl', 'fillgrid1', 'fldname', 'fldname', 'ADS column'),
('a__sl', 'fillgrid1', 'fldcaption', 'fldcaption', 'Caption'),
('a__sl', 'fillgrid1', 'hide', 'hide', 'Hide'),
('a__sl', 'fillgrid1', 'keyfield', 'keyfield', 'Is keyfield'),
('a__sl', 'fillgrid1', 'filter', 'filter', 'Filter'),
('a__sl', 'fillgrid1', 'sourcetable', 'srctrstruct', 'Source form'),
('a__sl', 'fillgrid1', 'sourcefld', 'srcfld', 'Source field'),
('a__sl', 'fillgrid1', 'datatypeui', 'datatypeui', 'Datatype'),
('pbill', 'FillGRNItems', 'pendingqty', 'qty', 'Bill Qty'),
('pbill', 'FillGRNItems', 'qty', 'grnqty', 'Qty'),
('pbill', 'FillGRNItems', 'grn_headerid', 'grn_headerid', 'hide'),
('pbill', 'FillGRNItems', 'taxcategory', 'taxcategory', 'Tax'),
('pbill', 'FillGRNItems', 'rate', 'rate', 'Rate'),
('pbill', 'FillGRNItems', 'grn_itemsid', 'grn_itemsid', 'hide'),
('pbill', 'FillGRNItems', 'itemname', 'itemname', 'Item Name'),
('pbill', 'FillGRNItems', 'pendingqty', 'pendingqty', 'Pending Qty'),
('pbill', 'FillGRNItems', 'grnno', 'gdocid', 'GRN No'),
('stkqc', 'QCItemsFilling', 'itemcode', 'itemcode', 'Product Code'),
('stkqc', 'QCItemsFilling', 'pending_qty', 'qty', 'Pending for QC'),
('stkqc', 'QCItemsFilling', 'itemdesc', 'itemname', 'Product Name'),
('stkqc', 'QCItemsFilling', 'stockrate', 'rate', 'Rate'),
('stkqc', 'QCItemsFilling', 'grn_itemsid', 'grn_itemsid', 'Hide'),
('stkqc', 'QCItemsFilling', 'batchserialbreakup', 'batchserialbreakup', 'Hide'),
('stkqc', 'QCItemsFilling', 'podocid', 'podocid', 'PO#'),
('stkqc', 'QCItemsFilling', 'purrqhdrid', 'purrqhdrid', 'Hide'),
('stkqc', 'QCItemsFilling', 'salesorder_itemsid', 'salesorder_itemsid', 'Hide'),
('stkqc', 'QCItemsFilling', 'salesorder_number', 'salesorder_number', 'Hide'),
('stkqc', 'QCItemsFilling', 'pending_qty', 'qpendingqty', 'Hide'),
('tsgrn', 'FillProducts', 'itemname', 'itemname', 'Item Name'),
('tsgrn', 'FillProducts', 'itemcode', 'itemcode', 'hide'),
('tsgrn', 'FillProducts', 'mrate', 'rate', 'mrate'),
('tsgrn', 'FillProducts', 'po_itemsid', 'po_itemsid', 'hide'),
('tsgrn', 'FillProducts', 'docid', 'podocid', 'Purchase Order#'),
('tsgrn', 'FillProducts', 'uom', 'uom', 'UOM'),
('tsgrn', 'FillProducts', 'qty', 'poqty', 'hide'),
('tsgrn', 'FillProducts', 'conversion_stockpurchase', 'conversion_stockpurchase', 'hide'),
('tsgrn', 'FillProducts', 'hsntaxrate', 'gstrate', 'hide'),
('tsgrn', 'FillProducts', 'pendingqty', 'pendingqty', 'Quantity'),
('tslpo', 'Fillitemdetails', 'rate', 'mrate', 'Hide'),
('tslpo', 'Fillitemdetails', 'uom', 'uom', 'UOM'),
('tslpo', 'Fillitemdetails', 'purrqhdrid', 'purrqhdrid', 'hide'),
('tslpo', 'Fillitemdetails', 'itemname', 'itemname', 'Product Name'),
('tslpo', 'Fillitemdetails', 'discount', 'discper', 'hide'),
('tslpo', 'Fillitemdetails', 'taxcategory', 'taxcategory', 'hide'),
('tslpo', 'Fillitemdetails', 'docid', 'prdocid', 'Purchase Req #'),
('tslpo', 'Fillitemdetails', 'itemcode', 'itemcode', 'Product Code'),
('tslpo', 'Fillitemdetails', 'taxableyn', 'taxableyn', 'hide'),
('tslpo', 'Fillitemdetails', 'hsntaxrate', 'taxper', 'hide'),
('tslpo', 'Fillitemdetails', 'pendingqty', 'qty', 'Pending Qty'),
('tslpo', 'Fillitemdetails', 'itemqty', 'prqty', 'Purchase Req. Qty'),
('tslpo', 'Fillitemdetails', 'purrqdtlid', 'purrqdtlid', 'hide'),
('tslpo', 'Fillitemdetails', 'salesorder_number', 'salesorder_number', 'Hide'),
('grnrt', 'fillgrid1', 'docid', 'grnno', 'GRN #'),
('grnrt', 'fillgrid1', 'itemname', 'itemname', 'Item Name'),
('grnrt', 'fillgrid1', 'itemcode', 'itemcode', 'hide'),
('grnrt', 'fillgrid1', 'uom', 'uom', 'UOM'),
('grnrt', 'fillgrid1', 'qty', 'grnqty', 'GRN Qty'),
('grnrt', 'fillgrid1', 'pendingqty', 'pendingqty', 'Pending Qty'),
('grnrt', 'fillgrid1', 'rate', 'rate', 'hide'),
('grnrt', 'fillgrid1', 'grn_itemsid', 'grn_itemsid', 'hide'),
('grnrt', 'fillgrid1', 'conversion_stockpurchase', 'conversion_stockpurchase', 'hide'),
('grnrt', 'fillgrid1', 'mrate', 'mrate', 'hide'),
('purre', 'fillgrid1', 'qcgrn_headerid', 'qcgrn_headerid', 'hide'),
('purre', 'fillgrid1', 'qcgrn_itemsid', 'qcgrn_itemsid', 'hide'),
('purre', 'fillgrid1', 'taxableyn', 'taxableyn', 'hide'),
('purre', 'fillgrid1', 'uom', 'uom', 'UOM'),
('purre', 'fillgrid1', 'purchase_bill_itemsid', 'purchase_bill_itemsid', 'hide'),
('purre', 'fillgrid1', 'taxcategory', 'taxcategory', 'Tax A/c'),
('purre', 'fillgrid1', 'rate', 'rate', 'Rate'),
('purre', 'fillgrid1', 'grn_itemsid', 'grn_itemsid', 'hide'),
('purre', 'fillgrid1', 'grnno', 'grndocid', 'GRN#'),
('purre', 'fillgrid1', 'taxcategory', 'taxcategorycode', 'Hide'),
('purre', 'fillgrid1', 'pbno', 'pbdocid', 'Purchase Bill#'),
('purre', 'fillgrid1', 'isinventory', 'isinventory', 'hide'),
('purre', 'fillgrid1', 'taxper', 'taxper', 'hide'),
('purre', 'fillgrid1', 'purchase_bill_headerid', 'purchase_bill_headerid', 'hide'),
('purre', 'fillgrid1', 'itemname', 'itemname', 'Item Name'),
('purre', 'fillgrid1', 'pendingqty', 'billqty', 'hide'),
('purre', 'fillgrid1', 'grn_headerid', 'grn_headerid', 'hide')
ON CONFLICT DO NOTHING;

-- Seed: axpfillgrid (9 rows)
INSERT INTO {schema}.axpfillgrid (tstruct, fgname, caption, fgsql, fromiview, tardc, multiselect, autoshow, srcdc, validat, exeonsave, firmbind, selecton, footer, valexpr, addrows, purpose, gfld, createdby, createdon, modifiedby, modifiedon, plist)
VALUES
('b_sql', 'fillgrid1', 'Fill ADS query columns', NULL, 'F', '2', 'F', 'T', '0', 'F', 'F', 'F', 'OnClick', NULL, NULL, '2', NULL, NULL, NULL, NULL, NULL, NULL, 'recid,sqlquerycols'),
('b_sql', 'fillgrid1', 'Fill ADS query columns', NULL, 'F', '2', 'F', 'T', '0', 'F', 'F', 'F', 'OnClick', NULL, NULL, '2', NULL, NULL, NULL, NULL, NULL, NULL, 'recid,sqlquerycols'),
('a__sl', 'fillgrid1', 'Fill ADS query columns', NULL, 'F', '2', 'F', 'F', '0', 'F', 'F', 'T', 'OnClick', NULL, NULL, '2', NULL, NULL, NULL, NULL, NULL, NULL, 'recid,sqlquerycols'),
('pbill', 'FillGRNItems', 'FillGRNItems', 'select a.grnno, a.itemname, a.qty, a.pendingqty, a.grn_headerid, a.grn_itemsid, a.taxcategory, a.rate from
(SELECT gh.docid as grnno, i.itemname, case when lower(gd.qcyesno) = ''yes'' then gd.qc_qty else gd.qty end as qty,
 case when lower(gd.qcyesno) = ''yes'' then (gd.qc_qty-gd.billqty-gd.grnreturnqty) else (gd.qty-gd.grnreturnqty-gd.billqty) end as pendingqty, 
gh.grn_headerid, gd.grn_itemsid, i.taxcategorycode taxcategory,gd.rate,gd.grn_itemsrow
  FROM grn_header gh
  join grn_items gd on ( gh.grn_headerid = gd.grn_headerid)  
  join vw_item i on ( gd.itemname=i.itemid)
  WHERE gh.cancel = ''F''     
    AND gh.supplier = :supplierid
    and gh.isfixedasset = :isfixedasset
    and gh.branch = cast( :branchid as numeric)
    and gh.app_desc = 1 
    and gh.currency =  :currency
    ) a        
where a.pendingqty > 0
order by grn_itemsrow
', 'F', '2', 'T', 'F', '0', 'F', 'F', 'F', 'OnClick', NULL, NULL, '1', NULL, NULL, NULL, NULL, NULL, NULL, 'supplierid,isfixedasset,branchid,currency'),
('stkqc', 'QCItemsFilling', 'QCItemsFilling', 'select a.itemdesc,b.podocid ,b.purrqhdrid ,b.salesorder_itemsid ,b.salesorder_number ,a.itemcode, b.grn_itemsid, b.stockrate,  (b.qty - b.qc_qty-b.grnreturnqty) + cast( :oldqty as numeric) as pending_qty,b.batchserialbreakup,b.expiry_date
from item a, grn_items b 
where a.company = :company
and a.cancel = ''F''
and a.active = ''T''
and a.itemid = b.itemname
and b.grn_headerid = :grn_headerid
and upper(b.qcyesno) = ''YES''
and (((b.qty - b.qc_qty-b.grnreturnqty) + cast( :oldqty as numeric)  > 0 or :recordid > 0) )
order by itemdesc
', 'F', '2', 'F', 'T', '0', 'T', 'F', 'F', NULL, NULL, NULL, '2', NULL, NULL, NULL, NULL, NULL, NULL, 'oldqty,company,grn_headerid,recordid'),
('tsgrn', 'FillProducts', 'Fill Products', NULL, 'F', '2', 'T', 'F', '0', 'F', 'F', 'F', 'OnClick', NULL, NULL, '1', NULL, NULL, NULL, NULL, NULL, NULL, 'company,branch,supplierid,currency'),
('tslpo', 'Fillitemdetails', 'Fill Item Details', NULL, 'F', '2', 'T', 'F', '0', 'F', 'F', 'T', 'OnClick', 'Total Qty. :itemqty', NULL, '1', 'discount amount in the query ?


SELECT a.purrqhdrid, a.docid,cast('''' as character varying) as Qtnno, c.itemname, c.itemcode, c.taxableyn, c.hsntaxrate, 
c.uom, b.itemqty, (b.itemqty - b.poqty) as pendingqty, b.purrqdtlid, cast(0 as numeric) as qtnhdrid, cast(0 as numeric) as qtndtlid, 
cast(0 as numeric(15,2)) as gstrate,cast('''' as character varying) as taxcategory, 
cast(0 as numeric(15,2)) as rate,
cast(0 as numeric(15,2)) as discount
  FROM purrqhdr a, purrqdtl b, vw_item c
  WHERE a.cancel = ''F''
    AND a.app_desc in (1,9)
    AND a.purrqhdrid = b.purrqhdrid
    AND b.itemname = c.itemid
    AND (b.itemqty - b.poqty) > 0 
    AND a.company = :company
    AND a.branch = :branchid
    AND :against = ''Purchase Request''
union
SELECT a.purrqhdrid, a.docid,d.docid as Qtnno, c.itemname, c.itemcode, c.taxableyn, c.hsntaxrate, 
c.uom, b.itemqty, (b.itemqty - b.poqty) as pendingqty, b.purrqdtlid, d.supplierquotationhdrid as qtnhdrid,e.supplierquotationdtlid as qtndtlid,
e.gstrate,e.taxcategory , 
e.rate, 
e.discount
  FROM purrqhdr a, purrqdtl b, vw_item c, supplierquotationhdr d, supplierquotationdtl e
  WHERE a.cancel = ''F''
    AND a.app_desc in (1,9)
    AND a.purrqhdrid = b.purrqhdrid
    AND b.itemname = c.itemid
    AND (b.itemqty - b.poqty) > 0 
    and d.supplierquotationhdrid=e.supplierquotationhdrid
    and  b.purrqdtlid=e.purrqdtlid
    and (e.appqty - e.poqty) >0
    AND :against = ''Quotation''
    AND a.company = :company
    AND a.branch = :branchid
ORDER BY docid, itemname
', NULL, NULL, NULL, NULL, NULL, 'warranty,supplierid,currency,company,branchid,against'),
('grnrt', 'fillgrid1', 'Fill Products', NULL, 'F', '2', 'T', 'F', '0', 'F', 'F', 'F', 'OnClick', NULL, NULL, '1', NULL, NULL, NULL, NULL, NULL, NULL, 'company,branchid,supplierid,currency'),
('purre', 'fillgrid1', 'Purchase Bill Products', NULL, 'F', '2', 'T', 'F', '0', 'T', 'F', 'F', NULL, NULL, NULL, '1', NULL, NULL, NULL, NULL, NULL, NULL, 'supply_type,supplierid,docdate,currency')
ON CONFLICT DO NOTHING;


-- Full golden-schema seed data split file.
SET LOCAL search_path = {schema}, pg_catalog;

