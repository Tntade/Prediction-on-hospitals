import argparse
import time
import os
from pathlib import Path

from loguru import logger
import yaml
from db_client import DBOperator

import sqls


def run(config, start_date, end_date, admdvs=None):
    """
    运行数据预处理
    Parameters:
    ----------------
    config: dict, 配置字典
    start_date: str, 开始月份，格式为'YYYY-MM-DD'
    end_date: str, 结束月份，格式为'YYYY-MM-DD'
    admdvs: str, 医保区划，默认不限医保区划
    """
    logger.info("开始数据预处理...")
    time1 = time.time()
    # 输入数据库及表
    source_db = config['db_tables']['input_schema']
    visit_settlement_table = config['db_tables']['register_table']
    prescription_detail_table = config['db_tables']['prescription_table']
    medical_ins_table = config['db_tables']['medins_table']
    # 输出数据库及表
    target_db = config['db_tables']['output_schema']
    kc21_table = config['temp_kc21_table']
    kc22_table = config['temp_kc22_table']

    if not admdvs or admdvs.strip().lower() in ('', 'all', 'null', 'none'):
        admdvs_cond = ''
    else:
        admdvs_cond = "and admdvs = '{}'".format(admdvs)   
        kc21_table += '_{}'.format(admdvs)
        kc22_table += '_{}'.format(admdvs)

    # 最小次数
    min_count = config['min_count']

    # 数据库类型与SQLs
    db_type = config['db_login_info']['type'].lower().strip()
    if db_type == 'hive':
        all_sqls = sqls.HiveSqls()
    elif db_type == 'odps':
        all_sqls = sqls.MaxComputeSqls()
    elif db_type == 'clickhouse':
        all_sqls = sqls.ClickhouseSqls()
    else:
        all_sqls = sqls.sql_classes[db_type]

    client = DBOperator(db_type, config['db_login_info'])
    # 输入kc21表数据准备
    client.execute("drop table if exists {target_db}.{kc21_table}".format(
        target_db=target_db, kc21_table=kc21_table))
    client.execute(all_sqls.generate_input_kc21_table.format(
        source_db=source_db, 
        visit_settlement_table=visit_settlement_table,
        medical_ins_table=medical_ins_table,
        target_db=target_db, 
        kc21_table=kc21_table,
        start_date=start_date, 
        end_date=end_date,
        admdvs_cond=admdvs_cond,
        min_count=min_count
    ))
    # 输入kc22表数据准备
    client.execute("drop table if exists {target_db}.{kc22_table}".format(
        target_db=target_db, kc22_table=kc22_table))
    client.execute(all_sqls.generate_input_kc22_table.format(
        source_db=source_db, 
        prescription_detail_table=prescription_detail_table,
        target_db=target_db, 
        kc21_table=kc21_table, 
        kc22_table=kc22_table
    ))
    
    logger.info('数据预处理完成，耗时{:.3f}s'.format(time.time() - time1))


if __name__ == '__main__':
    arg_parser = argparse.ArgumentParser()
    arg_parser.add_argument(
        "-c", "--config_file", type=str, help="配置文件路径"
    )
    arg_parser.add_argument(
        "--admdvs", type=str, help="医保区划，默认从配置文件获取"
    )
    arg_parser.add_argument(
        "--start_date", type=str, help="开始日期，格式为“YYYY-MM-DD”或“YYYYMMDD”"
    )
    arg_parser.add_argument(
        "--end_date", type=str, help="结束日期，格式为“YYYY-MM-DD”或“YYYYMMDD”"
    )
    args = arg_parser.parse_args()

    # 读取配置文件
    with open(args.config_file, encoding='utf-8') as config_file:
        config = yaml.safe_load(config_file)
    config.update(config['params']['rsk_crd_gtr'])

    log_file = Path(config['log_file']).expanduser().resolve()
    if not log_file.parent.exists():
        log_file.parent.mkdir(parents=True)
    logger.add(
        log_file, 
        backtrace=True, diagnose=True, rotation='1 days', retention='2 months'
    )

    logger.info('命令行参数：{}'.format(args))
    
    run(config, start_date=args.start_date, end_date=args.end_date, admdvs=args.admdvs)