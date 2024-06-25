import time
import datetime
import multiprocessing
import argparse
import traceback
import subprocess
import math
from pathlib import Path

from loguru import logger
import yaml
import pandas as pd
from db_client import DBOperator, LoggingToDb

from core_multicard_detection import run_multicard_detection
from utils import get_time_windows2
import data_prep2 as data_prep
from core_extract_result import extract_risk_result, RiskResultExtractor
from pyro_task_manager import get_task_manager
import sqls


def main(model_no, config, 
         start_date, end_date, admdvs=None, run_data_prep=True,
         distributed=False, 
         max_lock_num=12, 
         enable_local_workers=True):
    """
    卡聚集主函数
    Parameters:
    ----------------
    model_no: str, 模型编号
    config: dict, 配置字典
    start_date: str, 开始日期，格式为'YYYY-MM-DD'
    end_date: str, 结束日期，格式为'YYYY-MM-DD'
    admdvs: str, 医保区划，默认不限医保区划
    run_data_prep: bool, 是否运行数据预处理
    distributed: bool, 是否分布式运行
    max_lock_num: int, 分布式最大锁数量
    enable_local_workers: int, 开启本地workers
    """
    time1 = time.time()
    # 数据库
    input_schema = config['db_tables']['input_schema']
    output_schema = config['db_tables']['output_schema']
    # 输入表
    insurant_info_table = config['db_tables']['insurant_table']
    kc21_table = config['temp_kc21_table']
    kc22_table = config['temp_kc22_table']
    # 结果表
    risk_groups_table = config['risk_groups_table']
    risk_clinic_table = config['risk_clinic_table']
    risk_summary_table = config['risk_summary_table']
    risk_insurant_table = config['risk_insurant_table']
    risk_prescription_table = config['risk_prescription_table']
    risk_label_table = config['risk_label_table']
    risk_model_scores_table = config['risk_model_scores_table']
    # 若限定医保区划，则表名后添加后缀
    if admdvs is not None and admdvs.strip().lower() not in ('', 'all', 'null', 'none'):
        kc21_table += '_{}'.format(admdvs)
        kc22_table += '_{}'.format(admdvs)
        risk_groups_table += '_{}'.format(admdvs)

    db_type = config['db_login_info']['type'].lower().strip()
    assert db_type in ('clickhouse', 'hive', 'odps')

    # 获取每次执行数据的时间窗口（单位：月）
    time_windows = get_time_windows2(
        start_date, 
        end_date,
        config['window_size'], 
        config['step_size'])

    # SQLs
    if db_type == 'hive':
        all_sqls = sqls.HiveSqls()
    elif db_type == 'odps':
        all_sqls = sqls.MaxComputeSqls()
    elif db_type == 'clickhouse':
        all_sqls = sqls.ClickhouseSqls()
    else:
        all_sqls = sqls.sql_classes[db_type]

    sql_input_data = all_sqls.select_input_data
    sql_create_risk_groups_table = all_sqls.create_risk_groups_table

    # 如果只有一个月数据，则不使用分布式运行
    if len(time_windows) == 1:
        distributed = False

    # 分布式运行时创建task manager
    num_workers1 = 1
    if distributed:
        # 创建task manager
        task_manager = get_task_manager(
            "distributed.multicard_detection", max_lock_num=max_lock_num)
        # 创建本地workers
        if enable_local_workers:
            num_workers1 = min(
                len(time_windows), math.ceil(multiprocessing.cpu_count()//2))
            cmd = "python -u run_multicard_workers.py --num_workers {}".format(num_workers1)
            logger.info('开启本地pyro进程：{}'.format(cmd))
            subprocess.Popen(cmd, shell=True)
            time.sleep(0.5)

    # 数据预处理
    if run_data_prep:
        logger.info("开始数据预处理...")
        data_prep.run(config, start_date=start_date, end_date=end_date, admdvs=admdvs)
        logger.info('数据预处理完成，耗时{:.3f}s'.format(time.time() - time1))

    logger.info("开始执行卡聚集检测...") 
    # 每个时间窗口内，运行卡聚集函数
    if not distributed:
        # 非分布式
        result = []
        for idx, (start_date, end_date) in enumerate(time_windows):
            logger.info('start_date: {}, end_date: {}'.format(start_date, end_date))
            sql = sql_input_data.format(
                output_schema=output_schema,
                kc21_table=kc21_table,
                start_date=start_date,
                end_date=end_date
            )
            result.append(
                run_multicard_detection(
                    db_type,
                    config['db_login_info'],
                    sql,
                    start_date,
                    end_date,
                    admdvs,
                    time_interval=config['time_interval'],
                    min_count=config['min_count'], 
                    min_size=config['min_size'], 
                    max_size=config['max_size'],
                    min_jg_num=config['min_jg_num'],
                    min_person_ratio_in_subgroup=config['min_person_ratio_in_subgroup'],
                    min_risk_clinic_ratio_in_group=config['min_risk_clinic_ratio_in_group'],
                    resolution_parameter=config['resolution_parameter'],
                    n_jobs=multiprocessing.cpu_count()-1
                )
            )
            logger.info('total: {}, succeed: {}, elapse {:.3f}s'.format(
                len(time_windows), idx+1, time.time()-time1
            ))
    else:
        # 分布式
        for start_date, end_date in time_windows:
            logger.info('start_date: {}, end_date: {}'.format(start_date, end_date))
            sql = sql_input_data.format(
                output_schema=output_schema,
                kc21_table=kc21_table,
                start_date=start_date,
                end_date=end_date
            )
            task_manager.submit_task(
                args=(db_type, config['db_login_info'], sql, start_date, end_date, admdvs,), 
                kwargs={
                    'time_interval': config['time_interval'],
                    'min_count': config['min_count'], 
                    'min_size': config['min_size'], 
                    'max_size': config['max_size'],
                    'min_jg_num': config['min_jg_num'],
                    'min_person_ratio_in_subgroup': config['min_person_ratio_in_subgroup'],
                    'min_risk_clinic_ratio_in_group': config['min_risk_clinic_ratio_in_group'],
                    'resolution_parameter': config['resolution_parameter'],
                    'n_jobs': 1
                }
            )

        # 等待所有任务完成
        time2 = time.time() 
        logger.info('接收结果中...')   
        result = task_manager.get_all_results(timeout=86400)
        logger.info('结果接收完成，耗时{:.3f}s.'.format(time.time()-time2))

    # 结果合并，保存，后处理
    result = [i for i in result if i is not None]
    if not result:
        logger.warning('No result, please adjust parameters in configuration file!')
    else:
        result = pd.concat(result, ignore_index=True)
        result['model_no'] = model_no
        result['run_time'] = datetime.datetime.now()
        result['risk_clinic_ratio'] = result['risk_clinic_ratio'].fillna(0)
        result = result[[
            'model_no', 'run_time', 
            'input_admdvs', 'input_begndate', 'input_enddate', 
            'group_id', 'subgroup_id', 'risk_clinic_ratio', 
            'person_id', 'med_clinic_id',  
            'flx_med_org_id', 'med_type', 'adm_date', 'adm_time'
        ]]
        print('结果汇总：\n{}'.format(result))
        logger.info('result shape: {}'.format(result.shape))

        # 数据库操作器
        db_operator = DBOperator(db_type, config['db_login_info'])

        # 创建结果表并保存
        logger.info("Create result table in database...")
        db_operator.execute(sql_create_risk_groups_table.format(
            output_schema, risk_groups_table
        ))
        logger.info("Insert result into database...")
        db_operator.to_sql(
            result, database=output_schema, table_name=risk_groups_table
        )

        # 抽取风险结果
        # extract_risk_result(
        #     config['db_login_info'],
        #     kc21_table=output_schema + '.' + kc21_table,
        #     kc22_table=output_schema + '.' + kc22_table,
        #     insurant_info_table=input_schema + '.' + insurant_info_table,
        #     risk_groups_table=output_schema + '.' + risk_groups_table,
        #     risk_clinic_table=output_schema + '.' + risk_clinic_table,
        #     risk_summary_table=output_schema + '.' + risk_summary_table,
        #     risk_insurant_table=output_schema + '.' + risk_insurant_table,
        #     risk_prescription_table=output_schema + '.' + risk_prescription_table,
        #     risk_label_table=output_schema + '.' + risk_label_table
        # )
        # # 结果数量统计    
        # db_operator.read_sql("select count(1) from {} where model_no='{}'".format(
        #     output_schema + '.' + risk_summary_table, model_no))
        # db_operator.read_sql("select count(1) from {} where model_no='{}'".format(
        #     output_schema + '.' + risk_clinic_table, model_no))
        # db_operator.read_sql("select count(1) from {} where model_no='{}'".format(
        #     output_schema + '.' + risk_insurant_table, model_no))
        # db_operator.read_sql("select count(1) from {} where model_no='{}'".format(
        #     output_schema + '.' + risk_prescription_table, model_no))
        tables = {
            'register_table': output_schema + '.' + kc21_table,
            'prescription_table': output_schema + '.' + kc22_table,
            'insurant_table': input_schema + '.' + insurant_info_table,
            'risk_groups_table': output_schema + '.' + risk_groups_table,
            'risk_clinic_table': output_schema + '.' + risk_clinic_table,
            'risk_summary_table': output_schema + '.' + risk_summary_table,
            'risk_insurant_table': output_schema + '.' + risk_insurant_table,
            'risk_prescription_table': output_schema + '.' + risk_prescription_table,
            'risk_label_table': output_schema + '.' + risk_label_table,
            'risk_model_scores_table': output_schema + '.' + risk_model_scores_table
        }
        rre = RiskResultExtractor(config['db_login_info'], tables, model_no)
        rre.run()

        # 删除临时表
        db_operator.execute("drop table if exists {}.{}".format(
            output_schema, risk_groups_table))
        db_operator.execute("drop table if exists {}.{}".format(
            output_schema, kc21_table))
        db_operator.execute("drop table if exists {}.{}".format(
            output_schema, kc22_table))

    logger.info('卡聚集检测程序执行完成，耗时{:.3f}s.'.format(time.time()-time1))


if __name__ == '__main__':
    arg_parser = argparse.ArgumentParser()
    arg_parser.add_argument(
        "-c", "--config_file", type=str, default='config.yaml', 
        help="配置文件路径"
    )
    arg_parser.add_argument(
        "--admdvs", type=str, help="医保区划，默认从配置文件获取"
    )
    arg_parser.add_argument(
        "--start_date", type=str, 
        help="开始日期，格式为“YYYY-MM-DD”或“YYYYMMDD”"
    )
    arg_parser.add_argument(
        "--end_date", type=str, 
        help="结束日期，格式为“YYYY-MM-DD”或“YYYYMMDD”"
    )
    arg_parser.add_argument(
        "--formal_mode", type=int, default=1,
        help="正式运行模式：0. 测试阶段，不开启；1. 正式运行阶段，开启（默认）"
    )
    arg_parser.add_argument(
        "--run_data_prep", type=int, default=1, 
        help="是否运行数据预处理程序？"
    )
    arg_parser.add_argument(
        "--distributed", type=int, default=0, 
        help="是否分布式运行？ 0.否（默认），1.是。"
    )
    arg_parser.add_argument(
        "--enable_local_workers", type=int, default=1, 
        help="分布式运行时是否开启本地workers？ 0. 不开启，1.开启（默认）。"
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
        backtrace=True, diagnose=True,
        rotation='1 days', retention='2 months',
        enqueue=True
    )

    logger.info('命令行参数：{}'.format(args))

    time1 = time.time()  
    
    # 运行日志写入数据库
    # 运行状态: 1. 进行中，2. 运行失败，3. 运行成功
    log_to_db = LoggingToDb(
        config['db_login_info']['type'], 
        config['db_login_info'], 
        config['db_tables']['log_schema'], 
        config['db_tables']['log_table'], 
        model_abbr='rsk_crd_gtr', 
        model_name='卡聚集',
        formal_mode=args.formal_mode
    )    
    log_to_db.write_log(
        model_status='1', 
        start_date=args.start_date,
        end_date=args.end_date,
        admdvs=args.admdvs
    )
    model_no = log_to_db.model_no
    
    try:
        # 运行主程序
        main(
            model_no,
            config, 
            start_date=args.start_date, 
            end_date=args.end_date,
            admdvs=args.admdvs,
            run_data_prep=args.run_data_prep,
            distributed=args.distributed,
            max_lock_num=12,
            enable_local_workers=args.enable_local_workers
        )
    except Exception:
        logger.error(traceback.format_exc())
        log_to_db.write_log(
            model_status='2', 
            start_date=args.start_date,
            end_date=args.end_date,
            admdvs=args.admdvs
        )
    else:
        log_to_db.write_log(
            model_status='3', 
            start_date=args.start_date,
            end_date=args.end_date,
            admdvs=args.admdvs
        )

    logger.info('总耗时：{:.3f}s.'.format(time.time() - time1))
