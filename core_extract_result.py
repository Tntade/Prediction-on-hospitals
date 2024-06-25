import time

from loguru import logger

from db_client import DBOperator
from risk_general.risk_result.extract_risk_result import GeneralRiskResultExtractor

import sqls


class RiskResultExtractor(GeneralRiskResultExtractor):
    """
    风险结果抽取类:
    1. 抽取风险结果数据并写入相应风险结果表中
    2. 计算风险分
    """
    def __init__(self, db_login_info, tables, model_no):
        """
        Parameters:
        -----------------------------------
        db_login_info: dict, 数据库登录信息
        tables: dict, 各输入表和风险结果表，如：
            {
                'register_table': 'model_result.rsk_crd_gtr_kc21',
                'prescription_table': 'model_result.rsk_crd_gtr_kc22',
                'insurant_table': 'risk_model.rsk_cdm_psn',
                'risk_clinic_table': 'model_result.rsk_crd_gtr_clinic', 
                'risk_insurant_table': 'model_result.rsk_crd_gtr_insurant', 
                'risk_prescription_table': 'model_result.rsk_crd_gtr_prescription', 
                'risk_label_table': 'model_result.rsk_crd_gtr_label',
                'risk_groups_table': 'model_result.rsk_crd_gtr_groups',
                'risk_summary_table': 'model_result.rsk_crd_gtr_summary',
                'risk_model_scores_table': 'model_result.rsk_model_label'
            }
        model_no: str, 模型运行编号
        """
        super().__init__(db_login_info, tables, model_no)

        self.register_table = tables['register_table']
        self.insurant_table = tables['insurant_table']
        self.prescription_table = tables['prescription_table']
        self.risk_clinic_table = tables['risk_clinic_table']
        self.risk_insurant_table = tables['risk_insurant_table']
        self.risk_prescription_table = tables['risk_prescription_table']
        self.risk_label_table = tables['risk_label_table']
        self.risk_groups_table = tables['risk_groups_table']
        self.risk_summary_table = tables['risk_summary_table']
        self.risk_model_scores_table = tables['risk_model_scores_table']

        assert '.' in self.register_table
        assert '.' in self.insurant_table
        assert '.' in self.prescription_table
        assert '.' in self.risk_clinic_table
        assert '.' in self.risk_insurant_table
        assert '.' in self.risk_prescription_table
        assert '.' in self.risk_label_table
        assert '.' in self.risk_groups_table
        assert '.' in self.risk_summary_table
        assert '.' in self.risk_model_scores_table

        if self.db_type == 'hive':
            self.all_sqls = sqls.HiveSqls()
        elif self.db_type == 'odps':
            self.all_sqls = sqls.MaxComputeSqls()
        elif self.db_type == 'clickhouse':
            self.all_sqls = sqls.ClickhouseSqls()
        else:
            self.all_sqls = sqls.sql_classes[self.db_type]

    def extract_risk_clinic_result(self):
        """
        抽取风险就诊结果
        """
        # 创建风险就诊结果表并插入数据
        self.db_operator.execute(self.all_sqls.create_risk_clinic_table.format(
            risk_clinic_table=self.risk_clinic_table
        ))
        self.db_operator.execute(self.all_sqls.insert_risk_clinic_result.format(
            kc21_table=self.register_table,
            risk_groups_table=self.risk_groups_table,
            risk_clinic_table=self.risk_clinic_table
        ))
        # 统计结果数
        row_count = self.db_operator.read_sql(
            "select count(1) cnt from {} where model_no='{}'".format(
                self.risk_clinic_table, self.model_no)
        )['cnt'].iloc[0]
        logger.info("{} rows inserted into {}.".format(
            row_count, self.risk_clinic_table))

    def extract_risk_summary_result(self):
        """
        抽取风险总览表
        """
        # 风险总览表中间表
        temp_risk_summary_table = '{}_temp_{}'.format(self.risk_summary_table, int(time.time()))
        self.db_operator.execute(self.all_sqls.generate_temp_risk_summary_table.format(
            risk_groups_table=self.risk_groups_table,
            risk_clinic_table=self.risk_clinic_table,
            temp_risk_summary_table=temp_risk_summary_table
        ))
        # 创建风险总览结果表并插入数据
        self.db_operator.execute(self.all_sqls.create_risk_summary_table.format(
            risk_summary_table=self.risk_summary_table
        ))
        self.db_operator.execute(self.all_sqls.insert_risk_summary_result.format(
            risk_summary_table=self.risk_summary_table,
            temp_risk_summary_table=temp_risk_summary_table
        ))
        # 删除风险总览临时表
        self.db_operator.execute("drop table {}".format(temp_risk_summary_table))
        # 统计结果数
        row_count = self.db_operator.read_sql(
            "select count(1) cnt from {} where model_no='{}'".format(
                self.risk_summary_table, self.model_no)
        )['cnt'].iloc[0]
        logger.info("{} rows inserted into {}.".format(
            row_count, self.risk_summary_table))

    def extract_risk_label_result(self):
        """
        抽取风险标签结果
        """
        # 创建风险标签表并插入数据
        self.db_operator.execute(self.all_sqls.create_risk_label_table.format(
            risk_label_table=self.risk_label_table
        ))
        self.db_operator.execute(self.all_sqls.insert_risk_label_result.format(
            kc21_table=self.register_table,
            risk_groups_table=self.risk_groups_table,
            risk_label_table=self.risk_label_table,
            insurant_info_table=self.insurant_table,
            risk_summary_table=self.risk_summary_table
        ))
        # 统计结果数
        row_count = self.db_operator.read_sql(
            "select count(1) cnt from {} where model_no='{}'".format(
                self.risk_label_table, self.model_no)
        )['cnt'].iloc[0]
        logger.info("{} rows inserted into {}.".format(
            row_count, self.risk_label_table))

    def extract_risk_insurant_result(self):
        """
        抽取风险人群结果
        """
        # 创建风险人群结果表并插入数据
        self.db_operator.execute(self.all_sqls.create_risk_insurant_table.format(
            risk_insurant_table=self.risk_insurant_table
        ))
        self.db_operator.execute(self.all_sqls.insert_risk_insurant_result.format(
            insurant_info_table=self.insurant_table,
            risk_groups_table=self.risk_groups_table,
            risk_insurant_table=self.risk_insurant_table
        ))
        # 统计结果数
        row_count = self.db_operator.read_sql(
            "select count(1) cnt from {} where model_no='{}'".format(
                self.risk_insurant_table, self.model_no)
        )['cnt'].iloc[0]
        logger.info("{} rows inserted into {}.".format(
            row_count, self.risk_insurant_table))

    def extract_risk_prescription_result(self):
        """
        抽取风险处方明细结果
        """        
        super().extract_risk_prescription_result()

    def extract_risk_score_result(
            self, 
            alpha1=0.5, beta1=1, beta2=1, gamma1=10, 
            alpha2=0.5, beta3=0.1, beta4=0.01, gamma2=10):
        """
        抽取风险分结果
        """
        super().extract_risk_score_result(
            alpha1, beta1, beta2, gamma1,
            alpha2, beta3, beta4, gamma2
        )

    def run(self):
        self.extract_risk_clinic_result()
        self.extract_risk_summary_result()
        self.extract_risk_insurant_result()
        self.extract_risk_prescription_result()
        self.extract_risk_label_result()
        self.extract_risk_score_result()


def extract_risk_result(
        db_login_info,
        kc21_table, kc22_table, insurant_info_table,
        risk_groups_table, risk_clinic_table, risk_summary_table,
        risk_insurant_table, risk_prescription_table, risk_label_table):
    """
    hive中运行sql，从卡聚集风险结果表，关联挂号表、处方明细表、参保人表、医疗机构表，得到
    卡聚集就诊信息表、卡聚集风险总览表、卡聚集人员信息表、卡聚集处方详情表。
    Parameters:
    -----------------------------------
    db_login_info: dict, hive数据库连接信息
    kc21_table: 输入kc21表
    kc22_table: 输入kc22表
    insurant_info_table: 参保人信息表
    risk_groups_table: 风险组结果表
    risk_clinic_table: 风险就诊信息表
    risk_summary_table: 风险总览表
    risk_insurant_table: 风险人群信息表
    risk_prescription_table: 风险就诊处方明细表
    """
    logger.info('Post processing...')
    # 数据库连接和SQLs
    db_type = db_login_info['type'].strip().lower()
    client = DBOperator(db_type, db_login_info)
    if db_type == 'hive':
        all_sqls = sqls.HiveSqls()
    elif db_type == 'odps':
        all_sqls = sqls.MaxComputeSqls()
    elif db_type == 'clickhouse':
        all_sqls = sqls.ClickhouseSqls()
    else:
        all_sqls = sqls.sql_classes[db_type]
    
    # 创建风险就诊结果表并插入数据
    client.execute(all_sqls.create_risk_clinic_table.format(
        risk_clinic_table=risk_clinic_table
    ))
    client.execute(all_sqls.insert_risk_clinic_result.format(
        kc21_table=kc21_table,
        risk_groups_table=risk_groups_table,
        risk_clinic_table=risk_clinic_table
    ))
    # 风险总览表中间表
    temp_risk_summary_table = '{}_temp_{}'.format(risk_summary_table, int(time.time()))
    client.execute(all_sqls.generate_temp_risk_summary_table.format(
        risk_groups_table=risk_groups_table,
        risk_clinic_table=risk_clinic_table,
        temp_risk_summary_table=temp_risk_summary_table
    ))
    # 创建风险总览结果表并插入数据
    client.execute(all_sqls.create_risk_summary_table.format(
        risk_summary_table=risk_summary_table
    ))
    client.execute(all_sqls.insert_risk_summary_result.format(
        risk_summary_table=risk_summary_table,
        temp_risk_summary_table=temp_risk_summary_table
    ))
    # 删除风险总览临时表
    client.execute("drop table {}".format(temp_risk_summary_table))
    # 创建风险标签表并插入数据
    client.execute(all_sqls.create_risk_label_table.format(
        risk_label_table=risk_label_table
    ))
    client.execute(all_sqls.insert_risk_label_result.format(
        kc21_table=kc21_table,
        risk_groups_table=risk_groups_table,
        risk_label_table=risk_label_table,
        insurant_info_table=insurant_info_table,
        risk_summary_table=risk_summary_table
    ))
    # 创建风险人群结果表并插入数据
    client.execute(all_sqls.create_risk_insurant_table.format(
        risk_insurant_table=risk_insurant_table
    ))
    client.execute(all_sqls.insert_risk_insurant_result.format(
        insurant_info_table=insurant_info_table,
        risk_groups_table=risk_groups_table,
        risk_insurant_table=risk_insurant_table
    ))
    # 创建风险处方明细结果表并插入数据
    client.execute(all_sqls.create_risk_prescription_table.format(
        risk_prescription_table=risk_prescription_table
    ))
    client.execute(all_sqls.insert_risk_prescription_result.format(
        kc22_table=kc22_table,
        risk_groups_table=risk_groups_table,
        risk_prescription_table=risk_prescription_table
    ))

