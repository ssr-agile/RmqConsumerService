-- Auto-generated tenant migration from golden dump.
-- Runtime placeholders: {schema}, {schema_name}, {user_password}
SET LOCAL search_path = {schema}, pg_catalog;
SET LOCAL check_function_bodies = false;

ALTER TABLE ONLY {schema}.mg_account
    ADD CONSTRAINT aglmg_accountid PRIMARY KEY (mg_accountid);

ALTER TABLE ONLY {schema}.accountshdr
    ADD CONSTRAINT "UK_ACCOUNTHDR_DOCID" UNIQUE (docid);

ALTER TABLE ONLY {schema}.accountshdr
    ADD CONSTRAINT "UK_COMPANY_DOCID" UNIQUE (company, docid);

ALTER TABLE ONLY {schema}.accountsbrs
    ADD CONSTRAINT aglaccountsbrsid PRIMARY KEY (accountsbrsid);

ALTER TABLE ONLY {schema}.accountsdtl
    ADD CONSTRAINT aglaccountsdtlid PRIMARY KEY (accountsdtlid);

ALTER TABLE ONLY {schema}.accountshdr
    ADD CONSTRAINT aglaccountshdrid PRIMARY KEY (accountshdrid);

ALTER TABLE ONLY {schema}.add_ded_master
    ADD CONSTRAINT agladd_ded_masterid PRIMARY KEY (add_ded_masterid);

ALTER TABLE ONLY {schema}.apar_opening_balance
    ADD CONSTRAINT aglapar_opening_balanceid PRIMARY KEY (apar_opening_balanceid);

ALTER TABLE ONLY {schema}.arap_opening_balance
    ADD CONSTRAINT aglarap_opening_balanceid PRIMARY KEY (arap_opening_balanceid);

ALTER TABLE ONLY {schema}.arapadjustments
    ADD CONSTRAINT aglarapadjustmentsid PRIMARY KEY (arapadjustmentsid);

ALTER TABLE ONLY {schema}.arapdetails
    ADD CONSTRAINT aglarapdetailsid PRIMARY KEY (arapdetailsid);

ALTER TABLE ONLY {schema}.arappaydtl
    ADD CONSTRAINT aglarappaydtlid PRIMARY KEY (arappaydtlid);

ALTER TABLE ONLY {schema}.arappayhdr
    ADD CONSTRAINT aglarappayhdrid PRIMARY KEY (arappayhdrid);

ALTER TABLE ONLY {schema}.arreceiptdtl
    ADD CONSTRAINT aglarreceiptdtlid PRIMARY KEY (arreceiptdtlid);

ALTER TABLE ONLY {schema}.arreceipthdr
    ADD CONSTRAINT aglarreceipthdrid PRIMARY KEY (arreceipthdrid);

ALTER TABLE ONLY {schema}.ax_configure_fast_prints
    ADD CONSTRAINT aglax_configure_fast_printsid PRIMARY KEY (ax_configure_fast_printsid);

ALTER TABLE ONLY {schema}.axai_datasources_details
    ADD CONSTRAINT aglaxai_datasources_detailsid PRIMARY KEY (axai_datasources_detailsid);

ALTER TABLE ONLY {schema}.axai_templates
    ADD CONSTRAINT aglaxai_templatesid PRIMARY KEY (axai_templatesid);

ALTER TABLE ONLY {schema}.axautoprints
    ADD CONSTRAINT aglaxautoprintsid PRIMARY KEY (axautoprintsid);

ALTER TABLE ONLY {schema}.axcalendar
    ADD CONSTRAINT aglaxcalendarid PRIMARY KEY (axcalendarid);

ALTER TABLE ONLY {schema}.axcardtypemaster
    ADD CONSTRAINT aglaxcardtypemasterid PRIMARY KEY (axcardtypemasterid);

ALTER TABLE ONLY {schema}.axdef_newfield
    ADD CONSTRAINT aglaxdef_newfieldid PRIMARY KEY (axdef_newfieldid);

ALTER TABLE ONLY {schema}.axdirectsql_metadata
    ADD CONSTRAINT aglaxdirectsql_metadataid PRIMARY KEY (axdirectsql_metadataid);

ALTER TABLE ONLY {schema}.axdirectsql
    ADD CONSTRAINT aglaxdirectsqlid PRIMARY KEY (axdirectsqlid);

ALTER TABLE ONLY {schema}.axdsignconfig
    ADD CONSTRAINT aglaxdsignconfigid PRIMARY KEY (axdsignconfigid);

ALTER TABLE ONLY {schema}.axdsignmail
    ADD CONSTRAINT aglaxdsignmailid PRIMARY KEY (axdsignmailid);

ALTER TABLE ONLY {schema}.axentityrelations
    ADD CONSTRAINT aglaxentityrelationsid PRIMARY KEY (axentityrelationsid);

ALTER TABLE ONLY {schema}.axformnotify
    ADD CONSTRAINT aglaxformnotifyid PRIMARY KEY (axformnotifyid);

ALTER TABLE ONLY {schema}.axglovar
    ADD CONSTRAINT aglaxglovarid PRIMARY KEY (axglovarid);

ALTER TABLE ONLY {schema}.axgrouping
    ADD CONSTRAINT aglaxgroupingid PRIMARY KEY (axgroupingid);

ALTER TABLE ONLY {schema}.axgroupingmst
    ADD CONSTRAINT aglaxgroupingmstid PRIMARY KEY (axgroupingmstid);

ALTER TABLE ONLY {schema}.axiai_apikey_dtl
    ADD CONSTRAINT aglaxiai_apikey_dtlid PRIMARY KEY (axiai_apikey_dtlid);

ALTER TABLE ONLY {schema}.axiai_rbac_config
    ADD CONSTRAINT aglaxiai_rbac_configid PRIMARY KEY (axiai_rbac_configid);

ALTER TABLE ONLY {schema}.aximpdef
    ADD CONSTRAINT aglaximpdefid PRIMARY KEY (aximpdefid);

ALTER TABLE ONLY {schema}.axinqueues
    ADD CONSTRAINT aglaxinqueuesid PRIMARY KEY (axinqueuesid);

ALTER TABLE ONLY {schema}.axlanguage11x
    ADD CONSTRAINT aglaxlanguage11xid PRIMARY KEY (axlanguage11xid);

ALTER TABLE ONLY {schema}.axlov
    ADD CONSTRAINT aglaxlovid PRIMARY KEY (axlovid);

ALTER TABLE ONLY {schema}.axnotificationdef
    ADD CONSTRAINT aglaxnotificationdefid PRIMARY KEY (axnotificationdefid);

ALTER TABLE ONLY {schema}.axoutqueues
    ADD CONSTRAINT aglaxoutqueuesid PRIMARY KEY (axoutqueuesid);

ALTER TABLE ONLY {schema}.axoutqueuesmst
    ADD CONSTRAINT aglaxoutqueuesmstid PRIMARY KEY (axoutqueuesmstid);

ALTER TABLE ONLY {schema}.axp_appsearch_data_period
    ADD CONSTRAINT aglaxp_appsearch_data_periodid PRIMARY KEY (axp_appsearch_data_periodid);

ALTER TABLE ONLY {schema}.axp_appsearchdtl
    ADD CONSTRAINT aglaxp_appsearchdtlid PRIMARY KEY (axp_appsearchdtlid);

ALTER TABLE ONLY {schema}.axp_cards
    ADD CONSTRAINT aglaxp_cardsid PRIMARY KEY (axp_cardsid);

ALTER TABLE ONLY {schema}.axp_customdatatype
    ADD CONSTRAINT aglaxp_customdatatypeid PRIMARY KEY (axp_customdatatypeid);

ALTER TABLE ONLY {schema}.axp_dbwdetails
    ADD CONSTRAINT aglaxp_dbwdetailsid PRIMARY KEY (axp_dbwdetailsid);

ALTER TABLE ONLY {schema}.axp_tabledescriptor
    ADD CONSTRAINT aglaxp_tabledescriptorid PRIMARY KEY (axp_tabledescriptorid);

ALTER TABLE ONLY {schema}.axp_vp
    ADD CONSTRAINT aglaxp_vpid PRIMARY KEY (axp_vpid);

ALTER TABLE ONLY {schema}.axpdef_axcalendar_event
    ADD CONSTRAINT aglaxpdef_axcalendar_eventid PRIMARY KEY (axpdef_axcalendar_eventid);

ALTER TABLE ONLY {schema}.axpdef_axcalendar_eventstatus
    ADD CONSTRAINT aglaxpdef_axcalendar_eventstatusid PRIMARY KEY (axpdef_axcalendar_eventstatusid);

ALTER TABLE ONLY {schema}.axpdef_axpertapi
    ADD CONSTRAINT aglaxpdef_axpertapiid PRIMARY KEY (axpdef_axpertapiid);

ALTER TABLE ONLY {schema}.axpdef_axpertprops
    ADD CONSTRAINT aglaxpdef_axpertpropsid PRIMARY KEY (axpdef_axpertpropsid);

ALTER TABLE ONLY {schema}.axpdef_axvars_dbvar
    ADD CONSTRAINT aglaxpdef_axvars_dbvarid PRIMARY KEY (axpdef_axvars_dbvarid);

ALTER TABLE ONLY {schema}.axpdef_axvars
    ADD CONSTRAINT aglaxpdef_axvarsid PRIMARY KEY (axpdef_axvarsid);

ALTER TABLE ONLY {schema}.axpdef_impdata_templates
    ADD CONSTRAINT aglaxpdef_impdata_templatesid PRIMARY KEY (axpdef_impdata_templatesid);

ALTER TABLE ONLY {schema}.axpdef_jobs
    ADD CONSTRAINT aglaxpdef_jobsid PRIMARY KEY (axpdef_jobsid);

ALTER TABLE ONLY {schema}.axpdef_language
    ADD CONSTRAINT aglaxpdef_languageid PRIMARY KEY (axpdef_languageid);

ALTER TABLE ONLY {schema}.axpdef_news_events
    ADD CONSTRAINT aglaxpdef_news_eventsid PRIMARY KEY (axpdef_news_eventsid);

ALTER TABLE ONLY {schema}.axpdef_peg_actor
    ADD CONSTRAINT aglaxpdef_peg_actorid PRIMARY KEY (axpdef_peg_actorid);

ALTER TABLE ONLY {schema}.axpdef_peg_actorusergrp
    ADD CONSTRAINT aglaxpdef_peg_actorusergrpid PRIMARY KEY (axpdef_peg_actorusergrpid);

ALTER TABLE ONLY {schema}.axpdef_peg_grpfilter
    ADD CONSTRAINT aglaxpdef_peg_grpfilterid PRIMARY KEY (axpdef_peg_grpfilterid);

ALTER TABLE ONLY {schema}.axpdef_peg_processmaster
    ADD CONSTRAINT aglaxpdef_peg_processmasterid PRIMARY KEY (axpdef_peg_processmasterid);

ALTER TABLE ONLY {schema}.axpdef_peg_processmst_appr
    ADD CONSTRAINT aglaxpdef_peg_processmst_apprid PRIMARY KEY (axpdef_peg_processmst_apprid);

ALTER TABLE ONLY {schema}.axpdef_prcards
    ADD CONSTRAINT aglaxpdef_prcardsid PRIMARY KEY (axpdef_prcardsid);

ALTER TABLE ONLY {schema}.axpdef_publishapi
    ADD CONSTRAINT aglaxpdef_publishapiid PRIMARY KEY (axpdef_publishapiid);

ALTER TABLE ONLY {schema}.axpdef_ruleeng_expr
    ADD CONSTRAINT aglaxpdef_ruleeng_exprid PRIMARY KEY (axpdef_ruleeng_exprid);

ALTER TABLE ONLY {schema}.axpdef_ruleeng_fctl
    ADD CONSTRAINT aglaxpdef_ruleeng_fctlid PRIMARY KEY (axpdef_ruleeng_fctlid);

ALTER TABLE ONLY {schema}.axpdef_ruleeng_masks
    ADD CONSTRAINT aglaxpdef_ruleeng_masksid PRIMARY KEY (axpdef_ruleeng_masksid);

ALTER TABLE ONLY {schema}.axpdef_ruleeng_msg
    ADD CONSTRAINT aglaxpdef_ruleeng_msgid PRIMARY KEY (axpdef_ruleeng_msgid);

ALTER TABLE ONLY {schema}.axpdef_ruleeng_valexpr
    ADD CONSTRAINT aglaxpdef_ruleeng_valexprid PRIMARY KEY (axpdef_ruleeng_valexprid);

ALTER TABLE ONLY {schema}.axpdef_ruleeng
    ADD CONSTRAINT aglaxpdef_ruleengid PRIMARY KEY (axpdef_ruleengid);

ALTER TABLE ONLY {schema}.axpdef_script
    ADD CONSTRAINT aglaxpdef_scriptid PRIMARY KEY (axpdef_scriptid);

ALTER TABLE ONLY {schema}.axpdef_smartlist_charts
    ADD CONSTRAINT aglaxpdef_smartlist_chartsid PRIMARY KEY (axpdef_smartlist_chartsid);

ALTER TABLE ONLY {schema}.axpdef_smartlist_hlink
    ADD CONSTRAINT aglaxpdef_smartlist_hlinkid PRIMARY KEY (axpdef_smartlist_hlinkid);

ALTER TABLE ONLY {schema}.axpdef_smartlist_kpi
    ADD CONSTRAINT aglaxpdef_smartlist_kpiid PRIMARY KEY (axpdef_smartlist_kpiid);

ALTER TABLE ONLY {schema}.axpdef_smartlist_mdata
    ADD CONSTRAINT aglaxpdef_smartlist_mdataid PRIMARY KEY (axpdef_smartlist_mdataid);

ALTER TABLE ONLY {schema}.axpdef_smartlist
    ADD CONSTRAINT aglaxpdef_smartlistid PRIMARY KEY (axpdef_smartlistid);

ALTER TABLE ONLY {schema}.axpdef_usergroups
    ADD CONSTRAINT aglaxpdef_usergroupsid PRIMARY KEY (axpdef_usergroupsid);

ALTER TABLE ONLY {schema}.axpdef_userroles
    ADD CONSTRAINT aglaxpdef_userrolesid PRIMARY KEY (axpdef_userrolesid);

ALTER TABLE ONLY {schema}.axpeg_sendmsg
    ADD CONSTRAINT aglaxpeg_sendmsgid PRIMARY KEY (axpeg_sendmsgid);

ALTER TABLE ONLY {schema}.axperiodnotify
    ADD CONSTRAINT aglaxperiodnotifyid PRIMARY KEY (axperiodnotifyid);

ALTER TABLE ONLY {schema}.axpermissions
    ADD CONSTRAINT aglaxpermissionsid PRIMARY KEY (axpermissionsid);

ALTER TABLE ONLY {schema}.axpexchange
    ADD CONSTRAINT aglaxpexchangeid PRIMARY KEY (axpexchangeid);

ALTER TABLE ONLY {schema}.axprocessdef_delegation
    ADD CONSTRAINT aglaxprocessdef_delegationid PRIMARY KEY (axprocessdef_delegationid);

ALTER TABLE ONLY {schema}.axprocessdef
    ADD CONSTRAINT aglaxprocessdefid PRIMARY KEY (axprocessdefid);

ALTER TABLE ONLY {schema}.axprocessdefv2_escalation
    ADD CONSTRAINT aglaxprocessdefv2_escalationid PRIMARY KEY (axprocessdefv2_escalationid);

ALTER TABLE ONLY {schema}.axprocessdefv2_formctls
    ADD CONSTRAINT aglaxprocessdefv2_formctlsid PRIMARY KEY (axprocessdefv2_formctlsid);

ALTER TABLE ONLY {schema}.axprocessdefv2_reminder
    ADD CONSTRAINT aglaxprocessdefv2_reminderid PRIMARY KEY (axprocessdefv2_reminderid);

ALTER TABLE ONLY {schema}.axprocessdefv2_setval
    ADD CONSTRAINT aglaxprocessdefv2_setvalid PRIMARY KEY (axprocessdefv2_setvalid);

ALTER TABLE ONLY {schema}.axpstructconfig
    ADD CONSTRAINT aglaxpstructconfigid PRIMARY KEY (axpstructconfigid);

ALTER TABLE ONLY {schema}.axpstructconfigprops
    ADD CONSTRAINT aglaxpstructconfigpropsid PRIMARY KEY (axpstructconfigpropsid);

ALTER TABLE ONLY {schema}.axpstructconfigproval
    ADD CONSTRAINT aglaxpstructconfigprovalid PRIMARY KEY (axpstructconfigprovalid);

ALTER TABLE ONLY {schema}.axrulesdef_conmsg
    ADD CONSTRAINT aglaxrulesdef_conmsgid PRIMARY KEY (axrulesdef_conmsgid);

ALTER TABLE ONLY {schema}.axrulesdef_expr
    ADD CONSTRAINT aglaxrulesdef_exprid PRIMARY KEY (axrulesdef_exprid);

ALTER TABLE ONLY {schema}.axrulesdef_valexpr
    ADD CONSTRAINT aglaxrulesdef_valexprid PRIMARY KEY (axrulesdef_valexprid);

ALTER TABLE ONLY {schema}.axrulesdef
    ADD CONSTRAINT aglaxrulesdefid PRIMARY KEY (axrulesdefid);

ALTER TABLE ONLY {schema}.axuseractivations
    ADD CONSTRAINT aglaxuseractivationsid PRIMARY KEY (axuseractivationsid);

ALTER TABLE ONLY {schema}.axuserbranch
    ADD CONSTRAINT aglaxuserbranchid PRIMARY KEY (axuserbranchid);

ALTER TABLE ONLY {schema}.axusercharts
    ADD CONSTRAINT aglaxuserchartsid PRIMARY KEY (axuserchartsid);

ALTER TABLE ONLY {schema}.axuserdpermissions
    ADD CONSTRAINT aglaxuserdpermissionsid PRIMARY KEY (axuserdpermissionsid);

ALTER TABLE ONLY {schema}.axusergrouping
    ADD CONSTRAINT aglaxusergroupingid PRIMARY KEY (axusergroupingid);

ALTER TABLE ONLY {schema}.axusergroupsdetail
    ADD CONSTRAINT aglaxusergroupsdetailid PRIMARY KEY (axusergroupsdetailid);

ALTER TABLE ONLY {schema}.axuserpermissions
    ADD CONSTRAINT aglaxuserpermissionsid PRIMARY KEY (axuserpermissionsid);

ALTER TABLE ONLY {schema}.axvarcore
    ADD CONSTRAINT aglaxvarcoreid PRIMARY KEY (axvarcoreid);

ALTER TABLE ONLY {schema}.bank
    ADD CONSTRAINT aglbankid PRIMARY KEY (bankid);

ALTER TABLE ONLY {schema}.bankstatement
    ADD CONSTRAINT aglbankstatementid PRIMARY KEY (bankstatementid);

ALTER TABLE ONLY {schema}.batchandcost
    ADD CONSTRAINT aglbatchandcostid PRIMARY KEY (batchandcostid);

ALTER TABLE ONLY {schema}.batchmaster
    ADD CONSTRAINT aglbatchmasterid PRIMARY KEY (batchmasterid);

ALTER TABLE ONLY {schema}.billwiseopdtl
    ADD CONSTRAINT aglbillwiseopdtlid PRIMARY KEY (billwiseopdtlid);

ALTER TABLE ONLY {schema}.billwiseophdr
    ADD CONSTRAINT aglbillwiseophdrid PRIMARY KEY (billwiseophdrid);

ALTER TABLE ONLY {schema}.branch
    ADD CONSTRAINT aglbranchid PRIMARY KEY (branchid);

ALTER TABLE ONLY {schema}.brand_master
    ADD CONSTRAINT aglbrand_masterid PRIMARY KEY (brand_masterid);

ALTER TABLE ONLY {schema}.channel
    ADD CONSTRAINT aglchannelid PRIMARY KEY (channelid);

ALTER TABLE ONLY {schema}.city
    ADD CONSTRAINT aglcityid PRIMARY KEY (cityid);

ALTER TABLE ONLY {schema}.company
    ADD CONSTRAINT aglcompanyid PRIMARY KEY (companyid);

ALTER TABLE ONLY {schema}.costcentre_accountmapping
    ADD CONSTRAINT aglcostcentre_accountmappingid PRIMARY KEY (costcentre_accountmappingid);

ALTER TABLE ONLY {schema}.costcentre_apportiondetail
    ADD CONSTRAINT aglcostcentre_apportiondetailid PRIMARY KEY (costcentre_apportiondetailid);

ALTER TABLE ONLY {schema}.costcentre_data
    ADD CONSTRAINT aglcostcentre_dataid PRIMARY KEY (costcentre_dataid);

ALTER TABLE ONLY {schema}.costcentre_interface
    ADD CONSTRAINT aglcostcentre_interfaceid PRIMARY KEY (costcentre_interfaceid);

ALTER TABLE ONLY {schema}.country
    ADD CONSTRAINT aglcountryid PRIMARY KEY (countryid);

ALTER TABLE ONLY {schema}.currency
    ADD CONSTRAINT aglcurrencyid PRIMARY KEY (currencyid);

ALTER TABLE ONLY {schema}.customtypes
    ADD CONSTRAINT aglcustomtypesid PRIMARY KEY (customtypesid);

ALTER TABLE ONLY {schema}.deliverychallandtl
    ADD CONSTRAINT agldeliverychallandtlid PRIMARY KEY (deliverychallandtlid);

ALTER TABLE ONLY {schema}.deliverychallanhdr
    ADD CONSTRAINT agldeliverychallanhdrid PRIMARY KEY (deliverychallanhdrid);

ALTER TABLE ONLY {schema}.department
    ADD CONSTRAINT agldepartmentid PRIMARY KEY (departmentid);

ALTER TABLE ONLY {schema}.depositaccountdtl
    ADD CONSTRAINT agldepositaccountdtlid PRIMARY KEY (depositaccountdtlid);

ALTER TABLE ONLY {schema}.deposit
    ADD CONSTRAINT agldepositid PRIMARY KEY (depositid);

ALTER TABLE ONLY {schema}.designation
    ADD CONSTRAINT agldesignationid PRIMARY KEY (designationid);

ALTER TABLE ONLY {schema}.dimension_interface
    ADD CONSTRAINT agldimension_interfaceid PRIMARY KEY (dimension_interfaceid);

ALTER TABLE ONLY {schema}.dsignconfigdtl
    ADD CONSTRAINT agldsignconfigdtlid PRIMARY KEY (dsignconfigdtlid);

ALTER TABLE ONLY {schema}.dsignconfig
    ADD CONSTRAINT agldsignconfigid PRIMARY KEY (dsignconfigid);

ALTER TABLE ONLY {schema}.dwb_iviewscripts
    ADD CONSTRAINT agldwb_iviewscriptsid PRIMARY KEY (dwb_iviewscriptsid);

ALTER TABLE ONLY {schema}.dwb_publishprops
    ADD CONSTRAINT agldwb_publishpropsid PRIMARY KEY (dwb_publishpropsid);

ALTER TABLE ONLY {schema}.emaildef
    ADD CONSTRAINT aglemaildefid PRIMARY KEY (emaildefid);

ALTER TABLE ONLY {schema}.employee_master
    ADD CONSTRAINT aglemployee_masterid PRIMARY KEY (employee_masterid);

ALTER TABLE ONLY {schema}.executeapidef
    ADD CONSTRAINT aglexecuteapidefid PRIMARY KEY (executeapidefid);

ALTER TABLE ONLY {schema}.financialyeardtl
    ADD CONSTRAINT aglfinancialyeardtlid PRIMARY KEY (financialyeardtlid);

ALTER TABLE ONLY {schema}.financialyear
    ADD CONSTRAINT aglfinancialyearid PRIMARY KEY (financialyearid);

ALTER TABLE ONLY {schema}.fundtransfer
    ADD CONSTRAINT aglfundtransferid PRIMARY KEY (fundtransferid);

ALTER TABLE ONLY {schema}.gl_opening_balance
    ADD CONSTRAINT aglgl_opening_balanceid PRIMARY KEY (gl_opening_balanceid);

ALTER TABLE ONLY {schema}.globa1
    ADD CONSTRAINT aglgloba1id PRIMARY KEY (globa1id);

ALTER TABLE ONLY {schema}.global_mst_region
    ADD CONSTRAINT aglglobal_mst_regionid PRIMARY KEY (global_mst_regionid);

ALTER TABLE ONLY {schema}.grn_header
    ADD CONSTRAINT aglgrn_headerid PRIMARY KEY (grn_headerid);

ALTER TABLE ONLY {schema}.grn_items
    ADD CONSTRAINT aglgrn_itemsid PRIMARY KEY (grn_itemsid);

ALTER TABLE ONLY {schema}.grn_return_header
    ADD CONSTRAINT aglgrn_return_headerid PRIMARY KEY (grn_return_headerid);

ALTER TABLE ONLY {schema}.grn_return_items
    ADD CONSTRAINT aglgrn_return_itemsid PRIMARY KEY (grn_return_itemsid);

ALTER TABLE ONLY {schema}.groupbtns
    ADD CONSTRAINT aglgroupbtnsid PRIMARY KEY (groupbtnsid);

ALTER TABLE ONLY {schema}.gstde1
    ADD CONSTRAINT aglgstde1id PRIMARY KEY (gstde1id);

ALTER TABLE ONLY {schema}.hsnsac_codes
    ADD CONSTRAINT aglhsnsac_codesid PRIMARY KEY (hsnsac_codesid);

ALTER TABLE ONLY {schema}.hsntaxmapping
    ADD CONSTRAINT aglhsntaxmappingid PRIMARY KEY (hsntaxmappingid);

ALTER TABLE ONLY {schema}.htmlprint
    ADD CONSTRAINT aglhtmlprintid PRIMARY KEY (htmlprintid);

ALTER TABLE ONLY {schema}.htmlsections
    ADD CONSTRAINT aglhtmlsectionsid PRIMARY KEY (htmlsectionsid);

ALTER TABLE ONLY {schema}.iconfigurationdtl
    ADD CONSTRAINT agliconfigurationdtlid PRIMARY KEY (iconfigurationdtlid);

ALTER TABLE ONLY {schema}.iconfiguration
    ADD CONSTRAINT agliconfigurationid PRIMARY KEY (iconfigurationid);

ALTER TABLE ONLY {schema}.inventorytype
    ADD CONSTRAINT aglinventorytypeid PRIMARY KEY (inventorytypeid);

ALTER TABLE ONLY {schema}.invoiceheader
    ADD CONSTRAINT aglinvoiceheaderid PRIMARY KEY (invoiceheaderid);

ALTER TABLE ONLY {schema}.invoiceitems
    ADD CONSTRAINT aglinvoiceitemsid PRIMARY KEY (invoiceitemsid);

ALTER TABLE ONLY {schema}.itemaccountmapping
    ADD CONSTRAINT aglitemaccountmappingid PRIMARY KEY (itemaccountmappingid);

ALTER TABLE ONLY {schema}.itemcategory
    ADD CONSTRAINT aglitemcategoryid PRIMARY KEY (itemcategoryid);

ALTER TABLE ONLY {schema}.item
    ADD CONSTRAINT aglitemid PRIMARY KEY (itemid);

ALTER TABLE ONLY {schema}.location
    ADD CONSTRAINT agllocationid PRIMARY KEY (locationid);

ALTER TABLE ONLY {schema}.lv_basic
    ADD CONSTRAINT agllv_basicid PRIMARY KEY (lv_basicid);

ALTER TABLE ONLY {schema}.manpowerrate_master
    ADD CONSTRAINT aglmanpowerrate_masterid PRIMARY KEY (manpowerrate_masterid);

ALTER TABLE ONLY {schema}.mg_addressbook
    ADD CONSTRAINT aglmg_addressbookid PRIMARY KEY (mg_addressbookid);

ALTER TABLE ONLY {schema}.mg_coamastertemplate
    ADD CONSTRAINT aglmg_coamastertemplateid PRIMARY KEY (mg_coamastertemplateid);

ALTER TABLE ONLY {schema}.mg_customer
    ADD CONSTRAINT aglmg_customerid PRIMARY KEY (mg_customerid);

ALTER TABLE ONLY {schema}.mg_exrates
    ADD CONSTRAINT aglmg_exratesid PRIMARY KEY (mg_exratesid);

ALTER TABLE ONLY {schema}.mg_subledger
    ADD CONSTRAINT aglmg_subledgerid PRIMARY KEY (mg_subledgerid);

ALTER TABLE ONLY {schema}.mg_supplier
    ADD CONSTRAINT aglmg_supplierid PRIMARY KEY (mg_supplierid);

ALTER TABLE ONLY {schema}.mst_flatdiscount
    ADD CONSTRAINT aglmst_flatdiscountid PRIMARY KEY (mst_flatdiscountid);

ALTER TABLE ONLY {schema}.offsetdetail
    ADD CONSTRAINT agloffsetdetailid PRIMARY KEY (offsetdetailid);

ALTER TABLE ONLY {schema}.offsetheader
    ADD CONSTRAINT agloffsetheaderid PRIMARY KEY (offsetheaderid);

ALTER TABLE ONLY {schema}.pegts1
    ADD CONSTRAINT aglpegts1id PRIMARY KEY (pegts1id);

ALTER TABLE ONLY {schema}.pincode
    ADD CONSTRAINT aglpincodeid PRIMARY KEY (pincodeid);

ALTER TABLE ONLY {schema}.po_header
    ADD CONSTRAINT aglpo_headerid PRIMARY KEY (po_headerid);

ALTER TABLE ONLY {schema}.po_items
    ADD CONSTRAINT aglpo_itemsid PRIMARY KEY (po_itemsid);

ALTER TABLE ONLY {schema}.productcategory
    ADD CONSTRAINT aglproductcategoryid PRIMARY KEY (productcategoryid);

ALTER TABLE ONLY {schema}.purchase_bill_charges
    ADD CONSTRAINT aglpurchase_bill_chargesid PRIMARY KEY (purchase_bill_chargesid);

ALTER TABLE ONLY {schema}.purchase_bill_header
    ADD CONSTRAINT aglpurchase_bill_headerid PRIMARY KEY (purchase_bill_headerid);

ALTER TABLE ONLY {schema}.purchase_bill_items
    ADD CONSTRAINT aglpurchase_bill_itemsid PRIMARY KEY (purchase_bill_itemsid);

ALTER TABLE ONLY {schema}.purchasereturn_charges
    ADD CONSTRAINT aglpurchasereturn_chargesid PRIMARY KEY (purchasereturn_chargesid);

ALTER TABLE ONLY {schema}.purchasereturn_header
    ADD CONSTRAINT aglpurchasereturn_headerid PRIMARY KEY (purchasereturn_headerid);

ALTER TABLE ONLY {schema}.purchasereturn_items
    ADD CONSTRAINT aglpurchasereturn_itemsid PRIMARY KEY (purchasereturn_itemsid);

ALTER TABLE ONLY {schema}.purrqhdr
    ADD CONSTRAINT aglpurrqhdrid PRIMARY KEY (purrqhdrid);

ALTER TABLE ONLY {schema}.qcgrn_header
    ADD CONSTRAINT aglqcgrn_headerid PRIMARY KEY (qcgrn_headerid);

ALTER TABLE ONLY {schema}.qcgrn_items
    ADD CONSTRAINT aglqcgrn_itemsid PRIMARY KEY (qcgrn_itemsid);

ALTER TABLE ONLY {schema}.recon1
    ADD CONSTRAINT aglrecon1id PRIMARY KEY (recon1id);

ALTER TABLE ONLY {schema}.reference
    ADD CONSTRAINT aglreferenceid PRIMARY KEY (referenceid);

ALTER TABLE ONLY {schema}.salesorder_header
    ADD CONSTRAINT aglsalesorder_headerid PRIMARY KEY (salesorder_headerid);

ALTER TABLE ONLY {schema}.salesorder_items
    ADD CONSTRAINT aglsalesorder_itemsid PRIMARY KEY (salesorder_itemsid);

ALTER TABLE ONLY {schema}.salesreturns_header
    ADD CONSTRAINT aglsalesreturns_headerid PRIMARY KEY (salesreturns_headerid);

ALTER TABLE ONLY {schema}.salesreturns_items
    ADD CONSTRAINT aglsalesreturns_itemsid PRIMARY KEY (salesreturns_itemsid);

ALTER TABLE ONLY {schema}.saletype
    ADD CONSTRAINT aglsaletypeid PRIMARY KEY (saletypeid);

ALTER TABLE ONLY {schema}.searchcols
    ADD CONSTRAINT aglsearchcolsid PRIMARY KEY (searchcolsid);

ALTER TABLE ONLY {schema}.sect2
    ADD CONSTRAINT aglsect2id PRIMARY KEY (sect2id);

ALTER TABLE ONLY {schema}.sect4
    ADD CONSTRAINT aglsect4id PRIMARY KEY (sect4id);

ALTER TABLE ONLY {schema}.selco1
    ADD CONSTRAINT aglselco1id PRIMARY KEY (selco1id);

ALTER TABLE ONLY {schema}.sellingprice
    ADD CONSTRAINT aglsellingpriceid PRIMARY KEY (sellingpriceid);

ALTER TABLE ONLY {schema}.sendmsg
    ADD CONSTRAINT aglsendmsgid PRIMARY KEY (sendmsgid);

ALTER TABLE ONLY {schema}.service_bill_charges
    ADD CONSTRAINT aglservice_bill_chargesid PRIMARY KEY (service_bill_chargesid);

ALTER TABLE ONLY {schema}.service_bill_header
    ADD CONSTRAINT aglservice_bill_headerid PRIMARY KEY (service_bill_headerid);

ALTER TABLE ONLY {schema}.service_bill_items
    ADD CONSTRAINT aglservice_bill_itemsid PRIMARY KEY (service_bill_itemsid);

ALTER TABLE ONLY {schema}.service_invoice_charges
    ADD CONSTRAINT aglservice_invoice_chargesid PRIMARY KEY (service_invoice_chargesid);

ALTER TABLE ONLY {schema}.service_invoice_header
    ADD CONSTRAINT aglservice_invoice_headerid PRIMARY KEY (service_invoice_headerid);

ALTER TABLE ONLY {schema}.service_invoice_items
    ADD CONSTRAINT aglservice_invoice_itemsid PRIMARY KEY (service_invoice_itemsid);

ALTER TABLE ONLY {schema}.servicerate_master
    ADD CONSTRAINT aglservicerate_masterid PRIMARY KEY (servicerate_masterid);

ALTER TABLE ONLY {schema}.statea
    ADD CONSTRAINT aglstateaid PRIMARY KEY (stateaid);

ALTER TABLE ONLY {schema}.stateb
    ADD CONSTRAINT aglstatebid PRIMARY KEY (statebid);

ALTER TABLE ONLY {schema}.state
    ADD CONSTRAINT aglstateid PRIMARY KEY (stateid);

ALTER TABLE ONLY {schema}.stockclosing_posting
    ADD CONSTRAINT aglstockclosing_postingid PRIMARY KEY (stockclosing_postingid);

ALTER TABLE ONLY {schema}.stockissues_header
    ADD CONSTRAINT aglstockissues_headerid PRIMARY KEY (stockissues_headerid);

ALTER TABLE ONLY {schema}.stockissues_items
    ADD CONSTRAINT aglstockissues_itemsid PRIMARY KEY (stockissues_itemsid);

ALTER TABLE ONLY {schema}.stockopeningdtl
    ADD CONSTRAINT aglstockopeningdtlid PRIMARY KEY (stockopeningdtlid);

ALTER TABLE ONLY {schema}.stockopeninghdr
    ADD CONSTRAINT aglstockopeninghdrid PRIMARY KEY (stockopeninghdrid);

ALTER TABLE ONLY {schema}.stockreceipt_header
    ADD CONSTRAINT aglstockreceipt_headerid PRIMARY KEY (stockreceipt_headerid);

ALTER TABLE ONLY {schema}.stockreceipt_items
    ADD CONSTRAINT aglstockreceipt_itemsid PRIMARY KEY (stockreceipt_itemsid);

ALTER TABLE ONLY {schema}.stockvalue_summary
    ADD CONSTRAINT aglstockvalue_summaryid PRIMARY KEY (stockvalue_summaryid);

ALTER TABLE ONLY {schema}.stockvalue
    ADD CONSTRAINT aglstockvalueid PRIMARY KEY (stockvalueid);

ALTER TABLE ONLY {schema}.subledger_interface
    ADD CONSTRAINT aglsubledger_interfaceid PRIMARY KEY (subledger_interfaceid);

ALTER TABLE ONLY {schema}.supplierrate_header
    ADD CONSTRAINT aglsupplierrate_headerid PRIMARY KEY (supplierrate_headerid);

ALTER TABLE ONLY {schema}.taxmaster
    ADD CONSTRAINT agltaxmasterid PRIMARY KEY (taxmasterid);

ALTER TABLE ONLY {schema}.taxtypes
    ADD CONSTRAINT agltaxtypesid PRIMARY KEY (taxtypesid);

ALTER TABLE ONLY {schema}.tconfiguration
    ADD CONSTRAINT agltconfigurationid PRIMARY KEY (tconfigurationid);

ALTER TABLE ONLY {schema}.templates
    ADD CONSTRAINT agltemplatesid PRIMARY KEY (templatesid);

ALTER TABLE ONLY {schema}.transferreq_header
    ADD CONSTRAINT agltransferreq_headerid PRIMARY KEY (transferreq_headerid);

ALTER TABLE ONLY {schema}.tstruct_mst_details
    ADD CONSTRAINT agltstruct_mst_detailsid PRIMARY KEY (tstruct_mst_detailsid);

ALTER TABLE ONLY {schema}.uom
    ADD CONSTRAINT agluomid PRIMARY KEY (uomid);

ALTER TABLE ONLY {schema}.userquicklinks
    ADD CONSTRAINT agluserquicklinksid PRIMARY KEY (userquicklinksid);

ALTER TABLE ONLY {schema}.vouchertype
    ADD CONSTRAINT aglvouchertypeid PRIMARY KEY (vouchertypeid);

ALTER TABLE ONLY {schema}.withdrawalaccountdtl
    ADD CONSTRAINT aglwithdrawalaccountdtlid PRIMARY KEY (withdrawalaccountdtlid);

ALTER TABLE ONLY {schema}.withdrawal
    ADD CONSTRAINT aglwithdrawalid PRIMARY KEY (withdrawalid);

ALTER TABLE ONLY {schema}.ax_homebuild_master
    ADD CONSTRAINT ax_homebuild_master_pkey PRIMARY KEY (homebuild_id);

ALTER TABLE ONLY {schema}.ax_homebuild_saved
    ADD CONSTRAINT ax_homebuild_saved_pkey PRIMARY KEY (homebuild_id);

ALTER TABLE ONLY {schema}.ax_htmlplugins
    ADD CONSTRAINT ax_htmlplugins_pk PRIMARY KEY (name);

ALTER TABLE ONLY {schema}.ax_layoutdesign
    ADD CONSTRAINT ax_layoutdesign_pkey PRIMARY KEY (design_id);

ALTER TABLE ONLY {schema}.ax_layoutdesign_saved
    ADD CONSTRAINT ax_layoutdesign_saved_pkey PRIMARY KEY (design_id);

ALTER TABLE ONLY {schema}.ax_notify
    ADD CONSTRAINT ax_notify_pkey PRIMARY KEY (notification_id);

ALTER TABLE ONLY {schema}.ax_page_saved
    ADD CONSTRAINT ax_page_saved_pkey PRIMARY KEY (page_id);

ALTER TABLE ONLY {schema}.ax_page_templates
    ADD CONSTRAINT ax_page_templates_pkey PRIMARY KEY (template_id);

ALTER TABLE ONLY {schema}.ax_pages
    ADD CONSTRAINT ax_pages_pkey PRIMARY KEY (page_id);

ALTER TABLE ONLY {schema}.ax_widget
    ADD CONSTRAINT ax_widget_pkey PRIMARY KEY (widget_id);

ALTER TABLE ONLY {schema}.ax_widget_published
    ADD CONSTRAINT ax_widget_published_pkey PRIMARY KEY (widget_id);

ALTER TABLE ONLY {schema}.ax_widget_saved
    ADD CONSTRAINT ax_widget_saved_pkey PRIMARY KEY (widget_id);

ALTER TABLE ONLY {schema}.axaudit
    ADD CONSTRAINT axaudit_pkey PRIMARY KEY (sessionid);

ALTER TABLE ONLY {schema}.axcustomviews
    ADD CONSTRAINT axcustomviews_pkey PRIMARY KEY (name, username, transid, blobno);

ALTER TABLE ONLY {schema}.axerrorlog
    ADD CONSTRAINT axerrorlog_pkey PRIMARY KEY (username, eventdate);

ALTER TABLE ONLY {schema}.axi_command_prompts
    ADD CONSTRAINT axi_command_prompts_pkey PRIMARY KEY (id);

ALTER TABLE ONLY {schema}.axi_commands
    ADD CONSTRAINT axi_commands_pkey PRIMARY KEY (cmdtoken);

ALTER TABLE ONLY {schema}.axi_userfavourites
    ADD CONSTRAINT axi_userfavourites_pkey PRIMARY KEY (id);

ALTER TABLE ONLY {schema}.axiconmenu
    ADD CONSTRAINT axiconmenu_pkey PRIMARY KEY (parentpagename);

ALTER TABLE ONLY {schema}.axlanguage
    ADD CONSTRAINT axlanguage_pkey PRIMARY KEY (sname, lngname, compname);

ALTER TABLE ONLY {schema}.axlictrans
    ADD CONSTRAINT axlictrans_pkey PRIMARY KEY (licid);

ALTER TABLE ONLY {schema}.axp_smartviews_config
    ADD CONSTRAINT axp_smartviews_config_pkey PRIMARY KEY (username, ivname);

ALTER TABLE ONLY {schema}.axpages
    ADD CONSTRAINT axpages_pkey PRIMARY KEY (name, blobno);

ALTER TABLE ONLY {schema}.axpcal
    ADD CONSTRAINT axpcal_pkey PRIMARY KEY (axpcalid);

ALTER TABLE ONLY {schema}.axpclouddevsettings
    ADD CONSTRAINT axpclouddevsettings_tranid_type_key UNIQUE (tranid, type);

ALTER TABLE ONLY {schema}.axpdflanguage
    ADD CONSTRAINT axpdflanguage_pkey PRIMARY KEY (sname, lngname, compname);

ALTER TABLE ONLY {schema}.axprops
    ADD CONSTRAINT axprops_pkey PRIMARY KEY (name, blobno);

ALTER TABLE ONLY {schema}.axptree
    ADD CONSTRAINT axptree_pkey PRIMARY KEY (name, blobno);

ALTER TABLE ONLY {schema}.axpws_config
    ADD CONSTRAINT axpws_config_pkey PRIMARY KEY (transid, iviewname);

ALTER TABLE ONLY {schema}.axrequest
    ADD CONSTRAINT axrequest_pkey PRIMARY KEY (requestid);

ALTER TABLE ONLY {schema}.axresponse
    ADD CONSTRAINT axresponse_pkey PRIMARY KEY (responseid);

ALTER TABLE ONLY {schema}.axscheduler
    ADD CONSTRAINT axscheduler_pkey PRIMARY KEY (name, blobno);

ALTER TABLE ONLY {schema}.axuseraccess
    ADD CONSTRAINT axuseraccess_pkey PRIMARY KEY (rname, sname, stype);

ALTER TABLE ONLY {schema}.axusergroups
    ADD CONSTRAINT axusergroups_pkey PRIMARY KEY (groupname);

ALTER TABLE ONLY {schema}.axusers
    ADD CONSTRAINT axusers_pkey PRIMARY KEY (username);

ALTER TABLE ONLY {schema}.axuserspwdpolicy
    ADD CONSTRAINT axuserspwdpolicy_pkey PRIMARY KEY (username);

ALTER TABLE ONLY {schema}.axworkflow
    ADD CONSTRAINT axworkflow_pkey PRIMARY KEY (name);

ALTER TABLE ONLY {schema}.formsize
    ADD CONSTRAINT formsize_pkey PRIMARY KEY (username, formname);

ALTER TABLE ONLY {schema}.images
    ADD CONSTRAINT images_pkey PRIMARY KEY (name, blobno);

ALTER TABLE ONLY {schema}.iviews
    ADD CONSTRAINT iviews_pkey PRIMARY KEY (name, blobno);

ALTER TABLE ONLY {schema}.lviews
    ADD CONSTRAINT lviews_pkey PRIMARY KEY (name, username, transid, blobno);

ALTER TABLE ONLY {schema}.mg_account_template
    ADD CONSTRAINT mg_account_template_pk PRIMARY KEY (mg_account_templateid);

ALTER TABLE ONLY {schema}.mg_account_template
    ADD CONSTRAINT mg_account_template_unique UNIQUE (accountname);

ALTER TABLE ONLY {schema}.axattachworkflow
    ADD CONSTRAINT pk__axattach_wkidtransid PRIMARY KEY (wkid, transid);

ALTER TABLE ONLY {schema}.printprms
    ADD CONSTRAINT printprms_pkey PRIMARY KEY (iview);

ALTER TABLE ONLY {schema}.printprops
    ADD CONSTRAINT printprops_pkey PRIMARY KEY (name, blobno);

ALTER TABLE ONLY {schema}.prints
    ADD CONSTRAINT prints_pkey PRIMARY KEY (name, blobno);

ALTER TABLE ONLY {schema}.searchdef
    ADD CONSTRAINT searchdef_pkey PRIMARY KEY (transid, username);

ALTER TABLE ONLY {schema}.sequence
    ADD CONSTRAINT seq_unique UNIQUE (transtype, fieldname, prefix, prefixfield);

ALTER TABLE ONLY {schema}.sequence
    ADD CONSTRAINT sequence_pkey PRIMARY KEY (sequenceid);

ALTER TABLE ONLY {schema}.structlock
    ADD CONSTRAINT structlock_pkey PRIMARY KEY (sname);

ALTER TABLE ONLY {schema}.tstructs
    ADD CONSTRAINT tstructs_pkey PRIMARY KEY (name, blobno);

ALTER TABLE ONLY {schema}.stockvalue_summary
    ADD CONSTRAINT uk_stockvalue_summary_key UNIQUE (company, branch, location, itemname);

ALTER TABLE ONLY {schema}.axmmetadatamaster
    ADD CONSTRAINT unique_structtype_structname UNIQUE (structtype, structname);

ALTER TABLE ONLY {schema}.axi_userfavourites
    ADD CONSTRAINT uq_user_command UNIQUE (username, commandtext);

ALTER TABLE ONLY {schema}.ax_homebuild_responsibility
    ADD CONSTRAINT ax_homebuild_responsibility_homebuild_id_fkey FOREIGN KEY (homebuild_id) REFERENCES {schema}.ax_homebuild_master(homebuild_id) ON DELETE CASCADE;

ALTER TABLE ONLY {schema}.ax_homebuild_sd_responsibility
    ADD CONSTRAINT ax_homebuild_sd_responsibility_homebuild_id_fkey FOREIGN KEY (homebuild_id) REFERENCES {schema}.ax_homebuild_saved(homebuild_id) ON DELETE CASCADE;

ALTER TABLE ONLY {schema}.ax_hp_user_level_widget
    ADD CONSTRAINT ax_hp_user_level_widget_page_id_fkey FOREIGN KEY (page_id) REFERENCES {schema}.ax_pages(page_id) ON DELETE CASCADE;

ALTER TABLE ONLY {schema}.ax_page_responsibility
    ADD CONSTRAINT ax_page_responsibility_page_id_fkey FOREIGN KEY (page_id) REFERENCES {schema}.ax_pages(page_id) ON DELETE CASCADE;

ALTER TABLE ONLY {schema}.ax_page_sd_responsibility
    ADD CONSTRAINT ax_page_sd_responsibility_page_id_fkey FOREIGN KEY (page_id) REFERENCES {schema}.ax_page_saved(page_id) ON DELETE CASCADE;

ALTER TABLE ONLY {schema}.ax_widget_published
    ADD CONSTRAINT ax_widget_published_page_id_fkey FOREIGN KEY (page_id) REFERENCES {schema}.ax_pages(page_id) ON DELETE CASCADE;

ALTER TABLE ONLY {schema}.ax_widget_published
    ADD CONSTRAINT ax_widget_published_parent_widget_id_fkey FOREIGN KEY (parent_widget_id) REFERENCES {schema}.ax_widget_saved(widget_id) ON DELETE CASCADE;

ALTER TABLE ONLY {schema}.ax_widget_responsibility
    ADD CONSTRAINT ax_widget_responsibility_widget_id_fkey FOREIGN KEY (widget_id) REFERENCES {schema}.ax_widget(widget_id) ON DELETE CASCADE;

ALTER TABLE ONLY {schema}.ax_widget_saved
    ADD CONSTRAINT ax_widget_saved_page_id_fkey FOREIGN KEY (page_id) REFERENCES {schema}.ax_page_saved(page_id) ON DELETE CASCADE;

ALTER TABLE ONLY {schema}.axresponse
    ADD CONSTRAINT axresponse_requestid_fkey FOREIGN KEY (requestid) REFERENCES {schema}.axrequest(requestid);

ALTER TABLE ONLY {schema}.costcentre_accountmapping
    ADD CONSTRAINT fk_accountname_costs FOREIGN KEY (accountname) REFERENCES {schema}.mg_account(mg_accountid);

ALTER TABLE ONLY {schema}.billwiseophdr
    ADD CONSTRAINT fk_accountname_rpopn FOREIGN KEY (accountname) REFERENCES {schema}.mg_account(mg_accountid);

ALTER TABLE ONLY {schema}.gl_opening_balance
    ADD CONSTRAINT fk_accountname_vhngl FOREIGN KEY (accountname) REFERENCES {schema}.mg_account(mg_accountid);

ALTER TABLE ONLY {schema}.accountsdtl
    ADD CONSTRAINT fk_accountsdtl_accountname FOREIGN KEY (accountname) REFERENCES {schema}.mg_account(mg_accountid);

ALTER TABLE ONLY {schema}.accountsdtl
    ADD CONSTRAINT fk_accountsdtl_vchge FOREIGN KEY (acurrency) REFERENCES {schema}.currency(currencyid);

ALTER TABLE ONLY {schema}.mg_account
    ADD CONSTRAINT fk_acurrency_fiacc FOREIGN KEY (acurrency) REFERENCES {schema}.currency(currencyid);

ALTER TABLE ONLY {schema}.arapadjustments
    ADD CONSTRAINT fk_arapadjustments_accountname FOREIGN KEY (accountname) REFERENCES {schema}.mg_account(mg_accountid);

ALTER TABLE ONLY {schema}.arapdetails
    ADD CONSTRAINT fk_arapadjustments_subledgercode FOREIGN KEY (subledgercode) REFERENCES {schema}.mg_subledger(mg_subledgerid);

ALTER TABLE ONLY {schema}.arapdetails
    ADD CONSTRAINT fk_arapdetails_accountname FOREIGN KEY (accountname) REFERENCES {schema}.mg_account(mg_accountid);

ALTER TABLE ONLY {schema}.arapdetails
    ADD CONSTRAINT fk_arapdetails_subledgercode FOREIGN KEY (subledgercode) REFERENCES {schema}.mg_subledger(mg_subledgerid);

ALTER TABLE ONLY {schema}.bankstatement
    ADD CONSTRAINT fk_bankaccountname_banst FOREIGN KEY (bankaccountname) REFERENCES {schema}.mg_account(mg_accountid);

ALTER TABLE ONLY {schema}.service_invoice_header
    ADD CONSTRAINT fk_billtype_invsr FOREIGN KEY (billtype) REFERENCES {schema}.vouchertype(vouchertypeid);

ALTER TABLE ONLY {schema}.service_bill_header
    ADD CONSTRAINT fk_billtype_sbill FOREIGN KEY (billtype) REFERENCES {schema}.vouchertype(vouchertypeid);

ALTER TABLE ONLY {schema}.apar_opening_balance
    ADD CONSTRAINT fk_branch_aparo FOREIGN KEY (branch) REFERENCES {schema}.branch(branchid);

ALTER TABLE ONLY {schema}.arap_opening_balance
    ADD CONSTRAINT fk_branch_arapo FOREIGN KEY (branch) REFERENCES {schema}.branch(branchid);

ALTER TABLE ONLY {schema}.accountsbrs
    ADD CONSTRAINT fk_branch_bankr FOREIGN KEY (branch) REFERENCES {schema}.branch(branchid);

ALTER TABLE ONLY {schema}.arreceipthdr
    ADD CONSTRAINT fk_branch_crrec FOREIGN KEY (branch) REFERENCES {schema}.branch(branchid);

ALTER TABLE ONLY {schema}.deliverychallanhdr
    ADD CONSTRAINT fk_branch_dlchl FOREIGN KEY (branch) REFERENCES {schema}.branch(branchid);

ALTER TABLE ONLY {schema}.employee_master
    ADD CONSTRAINT fk_branch_empma FOREIGN KEY (branch) REFERENCES {schema}.branch(branchid);

ALTER TABLE ONLY {schema}.mg_customer
    ADD CONSTRAINT fk_branch_gcust FOREIGN KEY (branch) REFERENCES {schema}.branch(branchid);

ALTER TABLE ONLY {schema}.grn_return_header
    ADD CONSTRAINT fk_branch_grnrt FOREIGN KEY (branch) REFERENCES {schema}.branch(branchid);

ALTER TABLE ONLY {schema}.mg_supplier
    ADD CONSTRAINT fk_branch_gsupp FOREIGN KEY (branch) REFERENCES {schema}.branch(branchid);

ALTER TABLE ONLY {schema}.location
    ADD CONSTRAINT fk_branch_inloc FOREIGN KEY (branch) REFERENCES {schema}.branch(branchid);

ALTER TABLE ONLY {schema}.invoiceheader
    ADD CONSTRAINT fk_branch_invce FOREIGN KEY (branch) REFERENCES {schema}.branch(branchid);

ALTER TABLE ONLY {schema}.service_invoice_header
    ADD CONSTRAINT fk_branch_invsr FOREIGN KEY (branch) REFERENCES {schema}.branch(branchid);

ALTER TABLE ONLY {schema}.stockopeninghdr
    ADD CONSTRAINT fk_branch_itopn FOREIGN KEY (branch) REFERENCES {schema}.branch(branchid);

ALTER TABLE ONLY {schema}.mg_subledger
    ADD CONSTRAINT fk_branch_name_fisub FOREIGN KEY (branch_name) REFERENCES {schema}.branch(branchid);

ALTER TABLE ONLY {schema}.offsetheader
    ADD CONSTRAINT fk_branch_ofset FOREIGN KEY (branch) REFERENCES {schema}.branch(branchid);

ALTER TABLE ONLY {schema}.purchase_bill_header
    ADD CONSTRAINT fk_branch_pbill FOREIGN KEY (branch) REFERENCES {schema}.branch(branchid);

ALTER TABLE ONLY {schema}.purchasereturn_header
    ADD CONSTRAINT fk_branch_purre FOREIGN KEY (branch) REFERENCES {schema}.branch(branchid);

ALTER TABLE ONLY {schema}.billwiseophdr
    ADD CONSTRAINT fk_branch_rpopn FOREIGN KEY (branch) REFERENCES {schema}.branch(branchid);

ALTER TABLE ONLY {schema}.arappayhdr
    ADD CONSTRAINT fk_branch_rppay FOREIGN KEY (branch) REFERENCES {schema}.branch(branchid);

ALTER TABLE ONLY {schema}.salesreturns_header
    ADD CONSTRAINT fk_branch_salry FOREIGN KEY (branch) REFERENCES {schema}.branch(branchid);

ALTER TABLE ONLY {schema}.service_bill_header
    ADD CONSTRAINT fk_branch_sbill FOREIGN KEY (branch) REFERENCES {schema}.branch(branchid);

ALTER TABLE ONLY {schema}.salesorder_header
    ADD CONSTRAINT fk_branch_slord FOREIGN KEY (branch) REFERENCES {schema}.branch(branchid);

ALTER TABLE ONLY {schema}.qcgrn_header
    ADD CONSTRAINT fk_branch_stkqc FOREIGN KEY (branch) REFERENCES {schema}.branch(branchid);

ALTER TABLE ONLY {schema}.transferreq_header
    ADD CONSTRAINT fk_branch_trnrq FOREIGN KEY (branch) REFERENCES {schema}.branch(branchid);

ALTER TABLE ONLY {schema}.po_header
    ADD CONSTRAINT fk_branch_tslpo FOREIGN KEY (branch) REFERENCES {schema}.branch(branchid);

ALTER TABLE ONLY {schema}.deposit
    ADD CONSTRAINT fk_branch_vchdp FOREIGN KEY (branch) REFERENCES {schema}.branch(branchid);

ALTER TABLE ONLY {schema}.fundtransfer
    ADD CONSTRAINT fk_branch_vchft FOREIGN KEY (branch) REFERENCES {schema}.branch(branchid);

ALTER TABLE ONLY {schema}.withdrawal
    ADD CONSTRAINT fk_branch_vchwt FOREIGN KEY (branch) REFERENCES {schema}.branch(branchid);

ALTER TABLE ONLY {schema}.service_invoice_charges
    ADD CONSTRAINT fk_charge_invsr FOREIGN KEY (charge) REFERENCES {schema}.add_ded_master(add_ded_masterid);

ALTER TABLE ONLY {schema}.purchasereturn_charges
    ADD CONSTRAINT fk_charge_purre FOREIGN KEY (charge) REFERENCES {schema}.add_ded_master(add_ded_masterid);

ALTER TABLE ONLY {schema}.service_bill_charges
    ADD CONSTRAINT fk_charge_sbill FOREIGN KEY (charge) REFERENCES {schema}.add_ded_master(add_ded_masterid);

ALTER TABLE ONLY {schema}.dimension_interface
    ADD CONSTRAINT fk_company_dimas FOREIGN KEY (company) REFERENCES {schema}.company(companyid);

ALTER TABLE ONLY {schema}.mg_account
    ADD CONSTRAINT fk_company_fiacc FOREIGN KEY (company) REFERENCES {schema}.company(companyid);

ALTER TABLE ONLY {schema}.item
    ADD CONSTRAINT fk_company_prodm FOREIGN KEY (company) REFERENCES {schema}.company(companyid);

ALTER TABLE ONLY {schema}.branch
    ADD CONSTRAINT fk_companyname_bran FOREIGN KEY (companyname) REFERENCES {schema}.company(companyid);

ALTER TABLE ONLY {schema}.location
    ADD CONSTRAINT fk_companyname_inloc FOREIGN KEY (companyname) REFERENCES {schema}.company(companyid);

ALTER TABLE ONLY {schema}.costcentre_apportiondetail
    ADD CONSTRAINT fk_costcentrename_costs FOREIGN KEY (costcentrename) REFERENCES {schema}.costcentre_interface(costcentre_interfaceid);

ALTER TABLE ONLY {schema}.costcentre_accountmapping
    ADD CONSTRAINT fk_costgroups_costs FOREIGN KEY (costgroups) REFERENCES {schema}.dimension_interface(dimension_interfaceid);

ALTER TABLE ONLY {schema}.service_invoice_header
    ADD CONSTRAINT fk_currency_invsr FOREIGN KEY (currency) REFERENCES {schema}.currency(currencyid);

ALTER TABLE ONLY {schema}.purchasereturn_header
    ADD CONSTRAINT fk_currency_purre FOREIGN KEY (currency) REFERENCES {schema}.currency(currencyid);

ALTER TABLE ONLY {schema}.service_bill_header
    ADD CONSTRAINT fk_currency_sbill FOREIGN KEY (currency) REFERENCES {schema}.currency(currencyid);

ALTER TABLE ONLY {schema}.gl_opening_balance
    ADD CONSTRAINT fk_currency_vhngl FOREIGN KEY (currency) REFERENCES {schema}.currency(currencyid);

ALTER TABLE ONLY {schema}.invoiceheader
    ADD CONSTRAINT fk_customer_invce FOREIGN KEY (customer) REFERENCES {schema}.mg_customer(mg_customerid);

ALTER TABLE ONLY {schema}.service_invoice_header
    ADD CONSTRAINT fk_customer_invsr FOREIGN KEY (customer) REFERENCES {schema}.mg_customer(mg_customerid);

ALTER TABLE ONLY {schema}.salesreturns_header
    ADD CONSTRAINT fk_customer_salry FOREIGN KEY (customer) REFERENCES {schema}.mg_customer(mg_customerid);

ALTER TABLE ONLY {schema}.salesorder_header
    ADD CONSTRAINT fk_customer_slord FOREIGN KEY (customer) REFERENCES {schema}.mg_customer(mg_customerid);

ALTER TABLE ONLY {schema}.employee_master
    ADD CONSTRAINT fk_department_empma FOREIGN KEY (department) REFERENCES {schema}.department(departmentid);

ALTER TABLE ONLY {schema}.purchase_bill_items
    ADD CONSTRAINT fk_gdocid_pbill FOREIGN KEY (gdocid) REFERENCES {schema}.grn_header(grn_headerid);

ALTER TABLE ONLY {schema}.qcgrn_header
    ADD CONSTRAINT fk_grn_number_stkqc FOREIGN KEY (grn_number) REFERENCES {schema}.grn_header(grn_headerid);

ALTER TABLE ONLY {schema}.servicerate_master
    ADD CONSTRAINT fk_hsnaccode_serra FOREIGN KEY (hsnaccode) REFERENCES {schema}.hsnsac_codes(hsnsac_codesid);

ALTER TABLE ONLY {schema}.add_ded_master
    ADD CONSTRAINT fk_hsnno_tsads FOREIGN KEY (hsnno) REFERENCES {schema}.hsnsac_codes(hsnsac_codesid);

ALTER TABLE ONLY {schema}.hsntaxmapping
    ADD CONSTRAINT fk_hsnsaccode_hsnta FOREIGN KEY (hsnsaccode) REFERENCES {schema}.hsnsac_codes(hsnsac_codesid);

ALTER TABLE ONLY {schema}.item
    ADD CONSTRAINT fk_itembrand_prodm FOREIGN KEY (itembrand) REFERENCES {schema}.brand_master(brand_masterid);

ALTER TABLE ONLY {schema}.item
    ADD CONSTRAINT fk_itemcategory_prodm FOREIGN KEY (itemcategory) REFERENCES {schema}.itemcategory(itemcategoryid);

ALTER TABLE ONLY {schema}.deliverychallandtl
    ADD CONSTRAINT fk_itemname_dlchl FOREIGN KEY (itemname) REFERENCES {schema}.item(itemid);

ALTER TABLE ONLY {schema}.grn_return_items
    ADD CONSTRAINT fk_itemname_grnrt FOREIGN KEY (itemname) REFERENCES {schema}.item(itemid);

ALTER TABLE ONLY {schema}.stockreceipt_items
    ADD CONSTRAINT fk_itemname_instr FOREIGN KEY (itemname) REFERENCES {schema}.item(itemid);

ALTER TABLE ONLY {schema}.invoiceitems
    ADD CONSTRAINT fk_itemname_invce FOREIGN KEY (itemname) REFERENCES {schema}.item(itemid);

ALTER TABLE ONLY {schema}.stockissues_items
    ADD CONSTRAINT fk_itemname_marei FOREIGN KEY (itemname) REFERENCES {schema}.item(itemid);

ALTER TABLE ONLY {schema}.salesreturns_items
    ADD CONSTRAINT fk_itemname_salry FOREIGN KEY (itemname) REFERENCES {schema}.item(itemid);

ALTER TABLE ONLY {schema}.salesorder_items
    ADD CONSTRAINT fk_itemname_slord FOREIGN KEY (itemname) REFERENCES {schema}.item(itemid);

ALTER TABLE ONLY {schema}.qcgrn_items
    ADD CONSTRAINT fk_itemname_stkqc FOREIGN KEY (itemname) REFERENCES {schema}.item(itemid);

ALTER TABLE ONLY {schema}.transferreq_header
    ADD CONSTRAINT fk_itemname_trnrq FOREIGN KEY (itemname) REFERENCES {schema}.item(itemid);

ALTER TABLE ONLY {schema}.grn_items
    ADD CONSTRAINT fk_itemname_tsgrn FOREIGN KEY (itemname) REFERENCES {schema}.item(itemid);

ALTER TABLE ONLY {schema}.po_items
    ADD CONSTRAINT fk_itemname_tslpo FOREIGN KEY (itemname) REFERENCES {schema}.item(itemid);

ALTER TABLE ONLY {schema}.purrqhdr
    ADD CONSTRAINT fk_itemname_tsreq FOREIGN KEY (itemname) REFERENCES {schema}.item(itemid);

ALTER TABLE ONLY {schema}.deliverychallanhdr
    ADD CONSTRAINT fk_location_dlchl FOREIGN KEY (location) REFERENCES {schema}.location(locationid);

ALTER TABLE ONLY {schema}.grn_return_header
    ADD CONSTRAINT fk_location_grnrt FOREIGN KEY (location) REFERENCES {schema}.location(locationid);

ALTER TABLE ONLY {schema}.stockreceipt_header
    ADD CONSTRAINT fk_location_instr FOREIGN KEY (location) REFERENCES {schema}.location(locationid);

ALTER TABLE ONLY {schema}.stockopeninghdr
    ADD CONSTRAINT fk_location_itopn FOREIGN KEY (location) REFERENCES {schema}.location(locationid);

ALTER TABLE ONLY {schema}.stockissues_header
    ADD CONSTRAINT fk_location_marei FOREIGN KEY (location) REFERENCES {schema}.location(locationid);

ALTER TABLE ONLY {schema}.purchasereturn_header
    ADD CONSTRAINT fk_location_purre FOREIGN KEY (location) REFERENCES {schema}.location(locationid);

ALTER TABLE ONLY {schema}.salesreturns_header
    ADD CONSTRAINT fk_location_salry FOREIGN KEY (location) REFERENCES {schema}.location(locationid);

ALTER TABLE ONLY {schema}.sellingprice
    ADD CONSTRAINT fk_location_sepri FOREIGN KEY (location) REFERENCES {schema}.location(locationid);

ALTER TABLE ONLY {schema}.grn_header
    ADD CONSTRAINT fk_location_tsgrn FOREIGN KEY (location) REFERENCES {schema}.location(locationid);

ALTER TABLE ONLY {schema}.apar_opening_balance
    ADD CONSTRAINT fk_party_name_aparo FOREIGN KEY (party_name) REFERENCES {schema}.mg_supplier(mg_supplierid);

ALTER TABLE ONLY {schema}.arap_opening_balance
    ADD CONSTRAINT fk_party_name_arapo FOREIGN KEY (party_name) REFERENCES {schema}.mg_customer(mg_customerid);

ALTER TABLE ONLY {schema}.item
    ADD CONSTRAINT fk_productcategory_prodm FOREIGN KEY (productcategory) REFERENCES {schema}.productcategory(productcategoryid);

ALTER TABLE ONLY {schema}.productcategory
    ADD CONSTRAINT fk_productgroup_prcat FOREIGN KEY (productgroup) REFERENCES {schema}.itemcategory(itemcategoryid);

ALTER TABLE ONLY {schema}.service_bill_items
    ADD CONSTRAINT fk_purchase_account_sbill FOREIGN KEY (purchase_account) REFERENCES {schema}.mg_account(mg_accountid);

ALTER TABLE ONLY {schema}.deliverychallanhdr
    ADD CONSTRAINT fk_retail_customer_dlchl FOREIGN KEY (retail_customer) REFERENCES {schema}.mg_customer(mg_customerid);

ALTER TABLE ONLY {schema}.stockclosing_posting
    ADD CONSTRAINT fk_stockclosingposting_bran FOREIGN KEY (branch) REFERENCES {schema}.branch(branchid);

ALTER TABLE ONLY {schema}.mg_subledger
    ADD CONSTRAINT fk_subledger_company FOREIGN KEY (company) REFERENCES {schema}.company(companyid);

ALTER TABLE ONLY {schema}.mg_subledger
    ADD CONSTRAINT fk_subledger_controlaccount FOREIGN KEY (controlaccount) REFERENCES {schema}.mg_account(mg_accountid);

ALTER TABLE ONLY {schema}.apar_opening_balance
    ADD CONSTRAINT fk_subledgername_aparo FOREIGN KEY (subledgername) REFERENCES {schema}.mg_subledger(mg_subledgerid);

ALTER TABLE ONLY {schema}.arap_opening_balance
    ADD CONSTRAINT fk_subledgername_arapo FOREIGN KEY (subledgername) REFERENCES {schema}.mg_subledger(mg_subledgerid);

ALTER TABLE ONLY {schema}.billwiseophdr
    ADD CONSTRAINT fk_subledgername_rpopn FOREIGN KEY (subledgername) REFERENCES {schema}.mg_subledger(mg_subledgerid);

ALTER TABLE ONLY {schema}.accountsdtl
    ADD CONSTRAINT fk_subledgername_vchtr FOREIGN KEY (subledgername) REFERENCES {schema}.mg_subledger(mg_subledgerid);

ALTER TABLE ONLY {schema}.gl_opening_balance
    ADD CONSTRAINT fk_subledgername_vhngl FOREIGN KEY (subledgername) REFERENCES {schema}.mg_subledger(mg_subledgerid);

ALTER TABLE ONLY {schema}.stockclosing_posting
    ADD CONSTRAINT fk_subtypename_clpos FOREIGN KEY (subtypename) REFERENCES {schema}.vouchertype(vouchertypeid);

ALTER TABLE ONLY {schema}.gl_opening_balance
    ADD CONSTRAINT fk_subtypename_vhngl FOREIGN KEY (subtypename) REFERENCES {schema}.vouchertype(vouchertypeid);

ALTER TABLE ONLY {schema}.grn_return_header
    ADD CONSTRAINT fk_supplier_grnrt FOREIGN KEY (supplier) REFERENCES {schema}.mg_supplier(mg_supplierid);

ALTER TABLE ONLY {schema}.service_bill_header
    ADD CONSTRAINT fk_supplier_sbill FOREIGN KEY (supplier) REFERENCES {schema}.mg_supplier(mg_supplierid);

ALTER TABLE ONLY {schema}.fundtransfer
    ADD CONSTRAINT fk_tbranch_vchft FOREIGN KEY (tbranch) REFERENCES {schema}.branch(branchid);

ALTER TABLE ONLY {schema}.stockissues_header
    ADD CONSTRAINT fk_tobranch_sttro FOREIGN KEY (tobranch) REFERENCES {schema}.branch(branchid);

ALTER TABLE ONLY {schema}.stockissues_header
    ADD CONSTRAINT fk_tolocation_sttro FOREIGN KEY (tolocation) REFERENCES {schema}.location(locationid);
