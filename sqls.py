"""
sqls.py
SQLs configuration.
SQL语句变量命名约定：
1. 变量命名前缀
    create_: 建表语句
    create_temp_: 临时表建表语句
    drop_: 删表语句
    truncate_: 清空表语句
    insert_: insert语句
    generate_: 建表并插入数据，即create table ... as select ...
    generate_temp_: 临时表建表并插入数据
    select_: select语句
2. 变量命名后缀
    表名、表名缩写、数据内容和用途的简要描述性字符
"""


class HiveSqls:
    """
    Sqls for Hive
    """
    # 生成输入kc21表
    generate_input_kc21_table = """create table if not exists {target_db}.{kc21_table}
    row format delimited fields terminated by '\t'
    stored as orc tblproperties ("orc.compress"= "SNAPPY")
    as
    select t1.admdvs, t1.psn_no, t1.mdtrt_id, t1.med_type, 
        t1.fixmedins_code,
        t2.fixmedins_name,
        t1.adm_time, t1.dscg_time, t1.diag_code, t1.diag_name, 
        t1.medfee_sumamt, t1.hifp_payamt, t1.acct_payamt, t1.chfpdr_code,t1.chfpdr_name
    from (select substring(adm_time, 1, 7) adm_ym,
            admdvs, psn_no, mdtrt_id, med_type, fixmedins_code,
            adm_time, dscg_time, diag_code, diag_name, 
            medfee_sumamt, hifp_payamt, acct_payamt, chfpdr_code, chfpdr_name
        from {source_db}.{visit_settlement_table} 
        where med_type in ('11', '41')
        and to_date(adm_time) between to_date('{start_date}') and to_date('{end_date}')
        {admdvs_cond}) t1
    inner join (select substring(adm_time, 1, 7) adm_ym, psn_no
        from {source_db}.{visit_settlement_table} 
        where med_type in ('11', '41')
        and to_date(adm_time) between to_date('{start_date}') and to_date('{end_date}')
        {admdvs_cond}
        group by substring(adm_time, 1, 7), psn_no
        having count(1) >= {min_count}
    ) t3
    on t1.adm_ym = t3.adm_ym and t1.psn_no = t3.psn_no
    left join {source_db}.{medical_ins_table} t2
    on t1.fixmedins_code = t2.fixmedins_code
    """
    # 生成kc22输入表
    generate_input_kc22_table = """create table if not exists {target_db}.{kc22_table} 
    row format delimited fields terminated by '\t'
    stored as orc tblproperties ("orc.compress"= "SNAPPY")
    as
    select t1.mdtrt_id, t1.hilist_code, t1.hilist_name, t1.cnt, t1.det_item_fee_sumamt
    from {source_db}.{prescription_detail_table} t1
    inner join {target_db}.{kc21_table} t2
    on t1.mdtrt_id = t2.mdtrt_id
    """
    # 查询输入数据
    select_input_data = """select distinct 
        admdvs,
        mdtrt_id med_clinic_id, 
        psn_no person_id, 
        med_type med_type,
        fixmedins_code flx_med_org_id,
        adm_time,
        to_date(adm_time) adm_date
    from {output_schema}.{kc21_table}
    where med_type in ('11', '41')
    and to_date(adm_time) between to_date('{start_date}') and to_date('{end_date}')
    """

    # 创建风险组表
    create_risk_groups_table = """create table if not exists {}.{}(
        model_no string,
        run_time timestamp,
        input_admdvs string,
        input_begndate string,
        input_enddate string,
        group_id string,
        subgroup_id string,
        risk_clinic_ratio float,
        person_id string,
        med_clinic_id string,
        flx_med_org_id string,
        med_type string,
        adm_date string,
        adm_time timestamp
    )
    """
    # 创建风险就诊结果表
    create_risk_clinic_table = """CREATE TABLE if not exists {risk_clinic_table} (
        model_no string,
        input_admdvs string,
        input_begndate string,
        input_enddate string,
        risk_grp_no string,
        risk_sub_grp_no string,
        psn_no string,
        mdtrt_id string,
        admdvs string,
        fixmedins_code string,
        fixmedins_name string,
        adm_time timestamp,
        dscg_time timestamp,
        diag_code string,
        diag_name string,
        medfee_sumamt float,
        hifp_payamt float,
        acct_payamt float,
        crte_time timestamp,
        updt_time timestamp
    )
    """
    # 插入风险就诊结果
    insert_risk_clinic_result = """INSERT INTO {risk_clinic_table}
    select distinct t1.model_no,
        t1.input_admdvs,
        t1.input_begndate,
        t1.input_enddate,
        t1.group_id risk_grp_no,
        t1.subgroup_id risk_sub_grp_no,
        t1.person_id psn_no,
        t1.med_clinic_id mdtrt_id,
        t2.admdvs,
        t1.flx_med_org_id fixmedins_code,
        t2.fixmedins_name,
        t1.adm_time,
        t2.dscg_time,
        t2.diag_code,
        t2.diag_name,
        t2.medfee_sumamt,
        t2.hifp_payamt,
        t2.acct_payamt,
        t1.run_time crte_time,
        t1.run_time updt_time
    from {risk_groups_table} t1
    inner join {kc21_table} t2
    on t1.med_clinic_id = t2.mdtrt_id
    """
    # 生成风险总览临时表
    generate_temp_risk_summary_table = """CREATE TABLE {temp_risk_summary_table} as
    select t1.model_no, t1.input_admdvs, t1.input_begndate, t1.input_enddate, t1.risk_grp_no,
        size(collect_set(t1.risk_sub_grp_no)) risk_subgrp_cnt,
        size(collect_set(t1.psn_no)) risk_psn_cnt,
        size(collect_set(t1.fixmedins_code)) risk_medins_cnt,
        avg(t2.risk_clinic_ratio) risk_setl_rat,
        sum(t1.medfee_sumamt) medfee_sumamt,
        sum(t1.hifp_payamt) hifp_payamt,
        sum(t1.acct_payamt) acct_payamt
    from {risk_clinic_table} t1
    inner join (
        select distinct model_no, input_begndate, input_enddate, run_time, 
            group_id, risk_clinic_ratio
        from {risk_groups_table}) t2
    on t1.model_no=t2.model_no 
    and t1.input_begndate=t2.input_begndate and t1.input_enddate=t2.input_enddate
    and t1.crte_time=t2.run_time
    and t1.risk_grp_no=t2.group_id
    group by t1.model_no, t1.input_admdvs, t1.input_begndate, t1.input_enddate, t1.risk_grp_no
    """
    # 创建风险总览结果表
    create_risk_summary_table = """CREATE TABLE if not exists {risk_summary_table} (
        model_no string,
        input_admdvs string,
        input_begndate string,
        input_enddate string,
        risk_grp_no string,
        risk_subgrp_cnt int,
        risk_psn_cnt int,
        risk_medins_cnt int,
        risk_setl_rat float,
        medfee_sumamt float,
        hifp_payamt float,
        acct_payamt float,
        risk_subgrp_cnt_std float,
        risk_medins_cnt_std float,
        risk_psn_cnt_std float,
        risk_setl_rat_std float,
        risk_medfee_sumamt_std float,
        risk_sco float,
        crte_time timestamp,
        updt_time timestamp
    )
    """
    # 插入风险总览结果
    insert_risk_summary_result = """INSERT INTO {risk_summary_table} 
    select t1.model_no, input_admdvs, input_begndate, input_enddate, risk_grp_no,
        risk_subgrp_cnt, risk_psn_cnt, risk_medins_cnt, risk_setl_rat,
        medfee_sumamt, hifp_payamt, acct_payamt,
        risk_subgrp_cnt/max_risk_subgrp_cnt risk_subgrp_cnt_std,
        risk_medins_cnt/max_risk_medins_cnt risk_medins_cnt_std,
        risk_psn_cnt/max_risk_psn_cnt risk_psn_cnt_std,
        risk_setl_rat risk_setl_rat_std,
        medfee_sumamt/max_medfee_sumamt risk_medfee_sumamt_std,
        (risk_subgrp_cnt/max_risk_subgrp_cnt
            + risk_psn_cnt/max_risk_psn_cnt
            + risk_medins_cnt/max_risk_medins_cnt
            + risk_setl_rat
            + (hifp_payamt+acct_payamt)/medfee_sumamt) / 5 * 40 + 60 risk_sco,
        from_unixtime(unix_timestamp()) crte_time,
        from_unixtime(unix_timestamp()) updt_time
    from {temp_risk_summary_table} t1
    inner join (
        select model_no,
            max(risk_subgrp_cnt) max_risk_subgrp_cnt,
            max(risk_psn_cnt) max_risk_psn_cnt,
            max(risk_medins_cnt) max_risk_medins_cnt,
            max(medfee_sumamt) max_medfee_sumamt,
            max((hifp_payamt+acct_payamt)/medfee_sumamt) max_payamt_ratio
        from {temp_risk_summary_table}
        group by model_no
    ) t2
    on t1.model_no=t2.model_no
    """
    # 创建风险标签结果表
    create_risk_label_table = """CREATE TABLE if not exists {risk_label_table} (
        model_no           String,
        model_name         String,
        admdvs             String,
        input_begndate    date,
        input_enddate     date,
        risk_grp_no         String,
        mdtrt_id           String,
        psn_no             String,
        psn_name           String,
        chfpdr_code        String,
        chfpdr_name        String,
        fixmedins_code     String,
        fixmedins_name     String,
        medfee_sumamt      float,
        hifp_payamt        float,
        acct_payamt        float,
        rskfee_sumamt      float,
        grp_rsk_score      float,
        crte_time          timestamp,
        updt_time          timestamp
    )
    """
    # 插入风险标签结果
    insert_risk_label_result = """insert into {risk_label_table}
    select t1.model_no,'卡聚集' as model_name, t2.admdvs,
        date(t1.input_begndate) as input_begndate, date(t1.input_enddate) as input_enddate,
        t1.group_id risk_grp_no,
        t1.med_clinic_id mdtrt_id,
        t1.person_id psn_no,t3.psn_name,
        t2.chfpdr_code, t2.chfpdr_name,
        t1.flx_med_org_id fixmedins_code,
        t2.fixmedins_name,
        t2.medfee_sumamt,
        t2.hifp_payamt,
        t2.acct_payamt,
        t2.medfee_sumamt as rskfee_sumamt,
        t4.risk_sco as grp_rsk_score,
        t1.run_time crte_time,
        t1.run_time updt_time
    from {risk_groups_table} t1
    inner join {kc21_table} t2
    on t1.med_clinic_id = t2.mdtrt_id
    left join {insurant_info_table} t3
    on t1.person_id=t3.psn_no
    inner join {risk_summary_table} t4
    on t1.model_no = t4.model_no and t1.group_id = t4.risk_grp_no
    """

    # 创建风险人群结果表
    create_risk_insurant_table = """CREATE TABLE if not exists {risk_insurant_table} (
        model_no string,
        psn_no string,
        psn_name string,
        brdy string,
        gend string,
        live_addr string,
        emp_no string,
        crte_time timestamp,
        updt_time timestamp
    )
    """
    # 插入风险人群结果
    insert_risk_insurant_result = """INSERT INTO {risk_insurant_table}
    select t1.model_no, t1.psn_no, 
        t2.psn_name, t2.brdy, t2.gend, t2.live_addr, t2.emp_no,
        from_unixtime(unix_timestamp()) crte_time,
        from_unixtime(unix_timestamp()) updt_time
    from (select distinct model_no, person_id psn_no from {risk_groups_table}) t1
    inner join {insurant_info_table} t2
    on t1.psn_no=t2.psn_no
    """

    # 创建风险处方明细结果表
    create_risk_prescription_table = """CREATE TABLE if not exists {risk_prescription_table} (
        model_no string,
        mdtrt_id string,
        hilist_code string,
        hilist_name string,
        cnt float,
        det_item_fee_sumamt float,
        crte_time timestamp,
        updt_time timestamp
    )
    """
    # 插入风险处方明细结果
    insert_risk_prescription_result = """INSERT INTO {risk_prescription_table}
    select t1.model_no, t1.mdtrt_id, 
        t2.hilist_code, t2.hilist_name,
        sum(t2.cnt) cnt, 
        sum(t2.det_item_fee_sumamt) det_item_fee_sumamt,
        from_unixtime(unix_timestamp()) crte_time,
        from_unixtime(unix_timestamp()) updt_time
    from (select distinct model_no, med_clinic_id mdtrt_id from {risk_groups_table}) t1
    inner join {kc22_table} t2
    on t1.mdtrt_id=t2.mdtrt_id
    group by t1.model_no, t1.mdtrt_id, t2.hilist_code, t2.hilist_name
    """


class MaxComputeSqls:
    """
    Sqls for MaxCompute
    """
    # 生成输入kc21表
    generate_input_kc21_table = """create table if not exists {target_db}.{kc21_table} as
    select t1.admdvs, t1.psn_no, t1.mdtrt_id, t1.med_type, 
        t1.fixmedins_code,
        t2.fixmedins_name,
        t1.adm_time, t1.dscg_time, t1.diag_code, t1.diag_name, 
        t1.medfee_sumamt, t1.hifp_payamt, t1.acct_payamt, t1.chfpdr_code,t1.chfpdr_name
    from (select substring(adm_time, 1, 7) adm_ym,
            admdvs, psn_no, mdtrt_id, med_type, fixmedins_code,
            adm_time, dscg_time, diag_code, diag_name, 
            medfee_sumamt, hifp_payamt, acct_payamt, chfpdr_code, chfpdr_name
        from {source_db}.{visit_settlement_table} 
        where med_type in ('11', '41')
        and to_date(adm_time) between to_date('{start_date}') and to_date('{end_date}')
        {admdvs_cond}) t1
    inner join (select substring(adm_time, 1, 7) adm_ym, psn_no
        from {source_db}.{visit_settlement_table} 
        where med_type in ('11', '41')
        and to_date(adm_time) between to_date('{start_date}') and to_date('{end_date}')
        {admdvs_cond}
        group by substring(adm_time, 1, 7), psn_no
        having count(1) >= {min_count}
    ) t3
    on t1.adm_ym = t3.adm_ym and t1.psn_no = t3.psn_no
    left join {source_db}.{medical_ins_table} t2
    on t1.fixmedins_code = t2.fixmedins_code
    """
    # 生成kc22输入表
    generate_input_kc22_table = """create table if not exists {target_db}.{kc22_table} as
    select t1.mdtrt_id, t1.hilist_code, t1.hilist_name, t1.cnt, t1.det_item_fee_sumamt
    from {source_db}.{prescription_detail_table} t1
    inner join {target_db}.{kc21_table} t2
    on t1.mdtrt_id = t2.mdtrt_id
    """
    # 查询输入数据
    select_input_data = """select distinct 
        admdvs,
        mdtrt_id med_clinic_id, 
        psn_no person_id, 
        med_type med_type,
        fixmedins_code flx_med_org_id,
        adm_time,
        to_date(adm_time) adm_date
    from {output_schema}.{kc21_table}
    where med_type in ('11', '41')
    and to_date(adm_time) between to_date('{start_date}') and to_date('{end_date}')
    """

    # 创建风险组表
    create_risk_groups_table = """create table if not exists {}.{}(
        model_no string,
        run_time timestamp,
        input_admdvs string,
        input_begndate string,
        input_enddate string,
        group_id string,
        subgroup_id string,
        risk_clinic_ratio float,
        person_id string,
        med_clinic_id string,
        flx_med_org_id string,
        med_type string,
        adm_date string,
        adm_time timestamp
    )
    """
    # 创建风险就诊结果表
    create_risk_clinic_table = """CREATE TABLE if not exists {risk_clinic_table} (
        model_no string,
        input_admdvs string,
        input_begndate string,
        input_enddate string,
        risk_grp_no string,
        risk_sub_grp_no string,
        psn_no string,
        mdtrt_id string,
        admdvs string,
        fixmedins_code string,
        fixmedins_name string,
        adm_time timestamp,
        dscg_time timestamp,
        diag_code string,
        diag_name string,
        medfee_sumamt float,
        hifp_payamt float,
        acct_payamt float,
        crte_time timestamp,
        updt_time timestamp
    )
    """
    # 插入风险就诊结果
    insert_risk_clinic_result = """INSERT INTO {risk_clinic_table}
    select distinct t1.model_no,
        t1.input_admdvs,
        t1.input_begndate,
        t1.input_enddate,
        t1.group_id risk_grp_no,
        t1.subgroup_id risk_sub_grp_no,
        t1.person_id psn_no,
        t1.med_clinic_id mdtrt_id,
        t2.admdvs,
        t1.flx_med_org_id fixmedins_code,
        t2.fixmedins_name,
        t1.adm_time,
        t2.dscg_time,
        t2.diag_code,
        t2.diag_name,
        t2.medfee_sumamt,
        t2.hifp_payamt,
        t2.acct_payamt,
        t1.run_time crte_time,
        t1.run_time updt_time
    from {risk_groups_table} t1
    inner join {kc21_table} t2
    on t1.med_clinic_id = t2.mdtrt_id
    """
    # 生成风险总览临时表
    generate_temp_risk_summary_table = """CREATE TABLE {temp_risk_summary_table} as
    select t1.model_no, t1.input_admdvs, t1.input_begndate, t1.input_enddate, t1.risk_grp_no,
        size(collect_set(t1.risk_sub_grp_no)) risk_subgrp_cnt,
        size(collect_set(t1.psn_no)) risk_psn_cnt,
        size(collect_set(t1.fixmedins_code)) risk_medins_cnt,
        avg(t2.risk_clinic_ratio) risk_setl_rat,
        sum(t1.medfee_sumamt) medfee_sumamt,
        sum(t1.hifp_payamt) hifp_payamt,
        sum(t1.acct_payamt) acct_payamt
    from {risk_clinic_table} t1
    inner join (
        select distinct model_no, input_begndate, input_enddate, run_time, 
            group_id, risk_clinic_ratio
        from {risk_groups_table}) t2
    on t1.model_no=t2.model_no 
    and t1.input_begndate=t2.input_begndate and t1.input_enddate=t2.input_enddate
    and t1.crte_time=t2.run_time
    and t1.risk_grp_no=t2.group_id
    group by t1.model_no, t1.input_admdvs, t1.input_begndate, t1.input_enddate, t1.risk_grp_no
    """
    # 创建风险总览结果表
    create_risk_summary_table = """CREATE TABLE if not exists {risk_summary_table} (
        model_no string,
        input_admdvs string,
        input_begndate string,
        input_enddate string,
        risk_grp_no string,
        risk_subgrp_cnt int,
        risk_psn_cnt int,
        risk_medins_cnt int,
        risk_setl_rat float,
        medfee_sumamt float,
        hifp_payamt float,
        acct_payamt float,
        risk_subgrp_cnt_std float,
        risk_medins_cnt_std float,
        risk_psn_cnt_std float,
        risk_setl_rat_std float,
        risk_medfee_sumamt_std float,
        risk_sco float,
        crte_time timestamp,
        updt_time timestamp
    )
    """
    # 插入风险总览结果
    insert_risk_summary_result = """INSERT INTO {risk_summary_table} 
    select t1.model_no, input_admdvs, input_begndate, input_enddate, risk_grp_no,
        risk_subgrp_cnt, risk_psn_cnt, risk_medins_cnt, risk_setl_rat,
        medfee_sumamt, hifp_payamt, acct_payamt,
        risk_subgrp_cnt/max_risk_subgrp_cnt risk_subgrp_cnt_std,
        risk_medins_cnt/max_risk_medins_cnt risk_medins_cnt_std,
        risk_psn_cnt/max_risk_psn_cnt risk_psn_cnt_std,
        risk_setl_rat risk_setl_rat_std,
        medfee_sumamt/max_medfee_sumamt risk_medfee_sumamt_std,
        (risk_subgrp_cnt/max_risk_subgrp_cnt
            + risk_psn_cnt/max_risk_psn_cnt
            + risk_medins_cnt/max_risk_medins_cnt
            + risk_setl_rat
            + (hifp_payamt+acct_payamt)/medfee_sumamt) / 5 * 40 + 60 risk_sco,
        from_unixtime(unix_timestamp()) crte_time,
        from_unixtime(unix_timestamp()) updt_time
    from {temp_risk_summary_table} t1
    inner join (
        select model_no,
            max(risk_subgrp_cnt) max_risk_subgrp_cnt,
            max(risk_psn_cnt) max_risk_psn_cnt,
            max(risk_medins_cnt) max_risk_medins_cnt,
            max(medfee_sumamt) max_medfee_sumamt,
            max((hifp_payamt+acct_payamt)/medfee_sumamt) max_payamt_ratio
        from {temp_risk_summary_table}
        group by model_no
    ) t2
    on t1.model_no=t2.model_no
    """
    # 创建风险标签结果表
    create_risk_label_table = """CREATE TABLE if not exists {risk_label_table} (
        model_no           String,
        model_name         String,
        admdvs             String,
        input_begndate    date,
        input_enddate     date,
        risk_grp_no         String,
        mdtrt_id           String,
        psn_no             String,
        psn_name           String,
        chfpdr_code        String,
        chfpdr_name        String,
        fixmedins_code     String,
        fixmedins_name     String,
        medfee_sumamt      float,
        hifp_payamt        float,
        acct_payamt        float,
        rskfee_sumamt      float,
        grp_rsk_score      float,
        crte_time          timestamp,
        updt_time          timestamp
    )
    """
    # 插入风险标签结果
    insert_risk_label_result = """insert into {risk_label_table}
    select t1.model_no,'卡聚集' as model_name, t2.admdvs,
        date(t1.input_begndate) as input_begndate, date(t1.input_enddate) as input_enddate,
        t1.group_id risk_grp_no,
        t1.med_clinic_id mdtrt_id,
        t1.person_id psn_no,t3.psn_name,
        t2.chfpdr_code, t2.chfpdr_name,
        t1.flx_med_org_id fixmedins_code,
        t2.fixmedins_name,
        t2.medfee_sumamt,
        t2.hifp_payamt,
        t2.acct_payamt,
        t2.medfee_sumamt as rskfee_sumamt,
        t4.risk_sco as grp_rsk_score,
        t1.run_time crte_time,
        t1.run_time updt_time
    from {risk_groups_table} t1
    inner join {kc21_table} t2
    on t1.med_clinic_id = t2.mdtrt_id
    left join {insurant_info_table} t3
    on t1.person_id=t3.psn_no
    inner join {risk_summary_table} t4
    on t1.model_no = t4.model_no and t1.group_id = t4.risk_grp_no
    """

    # 创建风险人群结果表
    create_risk_insurant_table = """CREATE TABLE if not exists {risk_insurant_table} (
        model_no string,
        psn_no string,
        psn_name string,
        brdy string,
        gend string,
        live_addr string,
        emp_no string,
        crte_time timestamp,
        updt_time timestamp
    )
    """
    # 插入风险人群结果
    insert_risk_insurant_result = """INSERT INTO {risk_insurant_table}
    select t1.model_no, t1.psn_no, 
        t2.psn_name, t2.brdy, t2.gend, t2.live_addr, t2.emp_no,
        from_unixtime(unix_timestamp()) crte_time,
        from_unixtime(unix_timestamp()) updt_time
    from (select distinct model_no, person_id psn_no from {risk_groups_table}) t1
    inner join {insurant_info_table} t2
    on t1.psn_no=t2.psn_no
    """
    
    # 创建风险处方明细结果表
    create_risk_prescription_table = """CREATE TABLE if not exists {risk_prescription_table} (
        model_no string,
        mdtrt_id string,
        hilist_code string,
        hilist_name string,
        cnt float,
        det_item_fee_sumamt float,
        crte_time timestamp,
        updt_time timestamp
    )
    """
    # 插入风险处方明细结果
    insert_risk_prescription_result = """INSERT INTO {risk_prescription_table}
    select t1.model_no, t1.mdtrt_id, 
        t2.hilist_code, t2.hilist_name,
        sum(t2.cnt) cnt, 
        sum(t2.det_item_fee_sumamt) det_item_fee_sumamt,
        from_unixtime(unix_timestamp()) crte_time,
        from_unixtime(unix_timestamp()) updt_time
    from (select distinct model_no, med_clinic_id mdtrt_id from {risk_groups_table}) t1
    inner join {kc22_table} t2
    on t1.mdtrt_id=t2.mdtrt_id
    group by t1.model_no, t1.mdtrt_id, t2.hilist_code, t2.hilist_name
    """


class ClickhouseSqls:
    """
    Sqls for Clickhouse
    """
    # 生成输入kc21表
    generate_input_kc21_table = """create table if not exists {target_db}.{kc21_table} 
    engine = MergeTree() order by tuple() as
    select t1.admdvs admdvs, 
        t1.psn_no psn_no, 
        t1.mdtrt_id mdtrt_id, 
        t1.med_type med_type, 
        t1.fixmedins_code fixmedins_code,
        t2.fixmedins_name fixmedins_name,
        t1.adm_time adm_time, 
        t1.dscg_time dscg_time, 
        t1.diag_code diag_code, 
        t1.diag_name diag_name, 
        t1.medfee_sumamt medfee_sumamt, 
        t1.hifp_payamt hifp_payamt, 
        t1.acct_payamt acct_payamt, 
        t1.chfpdr_code chfpdr_code,
        t1.chfpdr_name chfpdr_name
    from (select toYYYYMM(toDate(adm_time, 'Asia/Shanghai')) adm_ym,
            admdvs, psn_no, mdtrt_id, med_type, fixmedins_code,
            adm_time, dscg_time, diag_code, diag_name, 
            medfee_sumamt, hifp_payamt, acct_payamt, chfpdr_code, chfpdr_name
        from {source_db}.{visit_settlement_table} 
        where med_type in ('11', '41')
        and toString(toDate(adm_time, 'Asia/Shanghai')) between '{start_date}' and '{end_date}' 
        {admdvs_cond}) t1
    inner join (select toYYYYMM(toDate(adm_time, 'Asia/Shanghai')) adm_ym, psn_no
        from {source_db}.{visit_settlement_table} 
        where med_type in ('11', '41')
        and toString(toDate(adm_time, 'Asia/Shanghai')) between '{start_date}' and '{end_date}' 
        {admdvs_cond}
        group by toYYYYMM(toDate(adm_time, 'Asia/Shanghai')), psn_no 
        having count(1) >= {min_count}
    ) t3 
    on t1.adm_ym = t3.adm_ym and t1.psn_no = t3.psn_no
    left join {source_db}.{medical_ins_table} t2
    on t1.fixmedins_code = t2.fixmedins_code
    """
    # 生成kc22输入表
    generate_input_kc22_table = """create table if not exists {target_db}.{kc22_table}
    engine = MergeTree() order by tuple() as
    select mdtrt_id, hilist_code, hilist_name, cnt, det_item_fee_sumamt
    from {source_db}.{prescription_detail_table}
    where mdtrt_id in (select distinct mdtrt_id from {target_db}.{kc21_table})
    """
    # 查询输入数据
    select_input_data = """select distinct 
        admdvs,
        mdtrt_id med_clinic_id, 
        psn_no person_id, 
        med_type med_type,
        fixmedins_code flx_med_org_id,
        toDateTime(adm_time, 'Asia/Shanghai') adm_time,
        toString(toDate(adm_time, 'Asia/Shanghai')) adm_date
    from {output_schema}.{kc21_table}
    where med_type in ('11', '41')
    and adm_date between '{start_date}' and '{end_date}'
    """

    # 创建风险组表
    create_risk_groups_table = """create table if not exists {}.{} (
        model_no String,
        run_time DateTime,
        input_admdvs Nullable(String),
        input_begndate String,
        input_enddate String,
        group_id String,
        subgroup_id String,
        risk_clinic_ratio Float32,
        person_id String,
        med_clinic_id String,
        flx_med_org_id String,
        med_type String,
        adm_date String,
        adm_time DateTime
    ) engine=MergeTree()
    order by tuple()
    """
    # 创建风险就诊结果表
    create_risk_clinic_table = """CREATE TABLE if not exists {risk_clinic_table} (
        model_no String,
        input_admdvs Nullable(String),
        input_begndate String,
        input_enddate String,
        risk_grp_no String,
        risk_sub_grp_no String,
        psn_no String,
        mdtrt_id String,
        admdvs Nullable(String),
        fixmedins_code Nullable(String),
        fixmedins_name Nullable(String),
        adm_time Nullable(DateTime),
        dscg_time Nullable(DateTime),
        diag_code Nullable(String),
        diag_name Nullable(String),
        medfee_sumamt Nullable(Float32),
        hifp_payamt Nullable(Float32),
        acct_payamt Nullable(Float32),
        crte_time DateTime,
        updt_time DateTime
    ) engine=MergeTree() order by tuple()
    """
    # 插入风险就诊结果
    insert_risk_clinic_result = """INSERT INTO {risk_clinic_table}
    select distinct t1.model_no,
        t1.input_admdvs,
        t1.input_begndate,
        t1.input_enddate,
        t1.group_id risk_grp_no,
        t1.subgroup_id risk_sub_grp_no,
        t1.person_id psn_no,
        t1.med_clinic_id mdtrt_id,
        t2.admdvs,
        t1.flx_med_org_id fixmedins_code,
        t2.fixmedins_name,
        t1.adm_time,
        t2.dscg_time,
        t2.diag_code,
        t2.diag_name,
        t2.medfee_sumamt,
        t2.hifp_payamt,
        t2.acct_payamt,
        t1.run_time crte_time,
        t1.run_time updt_time
    from {risk_groups_table} t1
    inner join {kc21_table} t2
    on t1.med_clinic_id = t2.mdtrt_id
    """
    # 生成风险总览临时表
    generate_temp_risk_summary_table = """CREATE TABLE {temp_risk_summary_table} engine=MergeTree() order by tuple() as
    select t1.model_no, t1.input_admdvs, t1.input_begndate, t1.input_enddate, t1.risk_grp_no,
        count(distinct t1.risk_sub_grp_no) risk_subgrp_cnt,
        count(distinct t1.psn_no) risk_psn_cnt,
        count(distinct t1.fixmedins_code) risk_medins_cnt,
        avg(t2.risk_clinic_ratio) risk_setl_rat,
        sum(t1.medfee_sumamt) medfee_sumamt,
        sum(t1.hifp_payamt) hifp_payamt,
        sum(t1.acct_payamt) acct_payamt
    from {risk_clinic_table} t1
    inner join (
        select distinct model_no, input_begndate, input_enddate, run_time, 
            group_id, risk_clinic_ratio
        from {risk_groups_table}) t2
    on t1.model_no=t2.model_no 
    and t1.input_begndate=t2.input_begndate and t1.input_enddate=t2.input_enddate
    and t1.crte_time=t2.run_time
    and t1.risk_grp_no=t2.group_id
    group by t1.model_no, t1.input_admdvs, t1.input_begndate, t1.input_enddate, t1.risk_grp_no
    """
    # 创建风险总览结果表
    create_risk_summary_table = """CREATE TABLE if not exists {risk_summary_table} (
        model_no String,
        input_admdvs Nullable(String),
        input_begndate String,
        input_enddate String,
        risk_grp_no String,
        risk_subgrp_cnt Nullable(Int),
        risk_psn_cnt Nullable(Int),
        risk_medins_cnt Nullable(Int),
        risk_setl_rat Nullable(Float32),
        medfee_sumamt Nullable(Float32),
        hifp_payamt Nullable(Float32),
        acct_payamt Nullable(Float32),
        risk_subgrp_cnt_std Nullable(Float32),
        risk_medins_cnt_std Nullable(Float32),
        risk_psn_cnt_std Nullable(Float32),
        risk_setl_rat_std Nullable(Float32),
        risk_medfee_sumamt_std Nullable(Float32),
        risk_sco Nullable(Float32),
        crte_time DateTime,
        updt_time DateTime
    ) engine=MergeTree() order by tuple()
    """
    # 插入风险总览结果
    insert_risk_summary_result = """INSERT INTO {risk_summary_table} 
    select t1.model_no, input_admdvs, input_begndate, input_enddate, risk_grp_no,
        risk_subgrp_cnt, risk_psn_cnt, risk_medins_cnt, risk_setl_rat,
        medfee_sumamt, hifp_payamt, acct_payamt,
        risk_subgrp_cnt/max_risk_subgrp_cnt risk_subgrp_cnt_std,
        risk_medins_cnt/max_risk_medins_cnt risk_medins_cnt_std,
        risk_psn_cnt/max_risk_psn_cnt risk_psn_cnt_std,
        risk_setl_rat risk_setl_rat_std,
        medfee_sumamt/max_medfee_sumamt risk_medfee_sumamt_std,
        (risk_subgrp_cnt/max_risk_subgrp_cnt
            + risk_psn_cnt/max_risk_psn_cnt
            + risk_medins_cnt/max_risk_medins_cnt
            + risk_setl_rat
            + (hifp_payamt+acct_payamt)/medfee_sumamt) / 5 * 40 + 60 risk_sco,
        now() crte_time,
        now() updt_time
    from {temp_risk_summary_table} t1
    inner join (
        select model_no,
            max(risk_subgrp_cnt) max_risk_subgrp_cnt,
            max(risk_psn_cnt) max_risk_psn_cnt,
            max(risk_medins_cnt) max_risk_medins_cnt,
            max(medfee_sumamt) max_medfee_sumamt,
            max((hifp_payamt+acct_payamt)/medfee_sumamt) max_payamt_ratio
        from {temp_risk_summary_table}
        group by model_no
    ) t2
    on t1.model_no=t2.model_no
    """
    # 创建风险标签结果表
    create_risk_label_table = """CREATE TABLE if not exists  {risk_label_table} (
        model_no           String,
        model_name         String,
        admdvs             String,
        input_begndate    date,
        input_enddate     date,
        risk_grp_no         String,
        mdtrt_id           String,
        psn_no             String,
        psn_name           Nullable(String),
        chfpdr_code        Nullable(String),
        chfpdr_name        Nullable(String),
        fixmedins_code     String,
        fixmedins_name     Nullable(String),
        medfee_sumamt      float,
        hifp_payamt        float,
        acct_payamt        float,
        rskfee_sumamt      float,
        grp_rsk_score      float,
        crte_time          timestamp,
        updt_time          timestamp
    ) engine=MergeTree() order by tuple()
    """
    # 插入风险标签结果
    insert_risk_label_result = """insert into {risk_label_table}
    select t1.model_no,'卡聚集' as model_name, t2.admdvs,
        date(t1.input_begndate) as input_begndate, date(t1.input_enddate) as input_enddate,
        t1.group_id risk_grp_no,
        t1.med_clinic_id mdtrt_id,
        t1.person_id psn_no,t3.psn_name,
        t2.chfpdr_code, t2.chfpdr_name,
        t1.flx_med_org_id fixmedins_code,
        t2.fixmedins_name,
        t2.medfee_sumamt,
        t2.hifp_payamt,
        t2.acct_payamt,
        t2.medfee_sumamt as rskfee_sumamt,
        t4.risk_sco as grp_rsk_score,
        t1.run_time crte_time,
        t1.run_time updt_time
    from {risk_groups_table} t1
    inner join {kc21_table} t2
    on t1.med_clinic_id = t2.mdtrt_id
    left join {insurant_info_table} t3
    on t1.person_id=t3.psn_no
    inner join {risk_summary_table} t4
    on t1.model_no = t4.model_no and t1.group_id = t4.risk_grp_no
    """

    # 创建风险人群结果表
    create_risk_insurant_table = """CREATE TABLE if not exists {risk_insurant_table} (
        model_no String,
        psn_no String,
        psn_name String,
        brdy Nullable(String),
        gend Nullable(String),
        live_addr Nullable(String),
        emp_no Nullable(String),
        crte_time DateTime,
        updt_time DateTime
    ) engine=MergeTree() order by tuple()
    """
    # 插入风险人群结果
    insert_risk_insurant_result = """INSERT INTO {risk_insurant_table}
    select t1.model_no, t1.psn_no, 
        t2.psn_name, t2.brdy, t2.gend, t2.live_addr, t2.emp_no,
        now() crte_time,
        now() updt_time
    from (select distinct model_no, person_id psn_no from {risk_groups_table}) t1
    inner join {insurant_info_table} t2
    on t1.psn_no=t2.psn_no
    """
    
    # 创建风险处方明细结果表
    create_risk_prescription_table = """CREATE TABLE if not exists {risk_prescription_table} (
        model_no String,
        mdtrt_id String,
        hilist_code Nullable(String),
        hilist_name Nullable(String),
        cnt Nullable(Float32),
        det_item_fee_sumamt Nullable(Float32),
        crte_time DateTime,
        updt_time DateTime
    ) engine=MergeTree() order by tuple()
    """
    # 插入风险处方明细结果
    insert_risk_prescription_result = """INSERT INTO {risk_prescription_table}
    select t1.model_no, t1.mdtrt_id, 
        t2.hilist_code, t2.hilist_name,
        sum(t2.cnt),
        sum(t2.det_item_fee_sumamt),
        now() crte_time,
        now() updt_time
    from (select distinct model_no, med_clinic_id mdtrt_id from {risk_groups_table}) t1
    inner join {kc22_table} t2
    on t1.mdtrt_id=t2.mdtrt_id
    group by t1.model_no, t1.mdtrt_id, t2.hilist_code, t2.hilist_name
    """


# sql类字典
sql_classes = {
    'hive': HiveSqls(),
    'odps': MaxComputeSqls(),
    'clickhouse': ClickhouseSqls()
}