create table daily_yield as (SELECT * FROM (select s_info_windcode,trade_dt,b_anal_yield_cnbd from cbondanalysiscnbd_his where CAST(trade_dt AS DATE) between '2019-01-01' and '2022-03-04') a LEFT JOIN (SELECT bond_code,sec_name, redemption_beginning, issue_firstissue, adj_rate_latestmir_cnbd, place, windl1type, windl2type, industry_theme, issue_issuemethod, clause, issuershortened, nature1 FROM bondinfo) b ON a.s_info_windcode = b.bond_code);