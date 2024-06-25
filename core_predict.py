import time
from collections import Counter

from loguru import logger
import numpy as np
import pandas as pd
import igraph as ig
from joblib import Parallel, delayed

from db_client import DBOperator


def run_multicard_detection(
        db_type, db_login_info, sql, start_date, end_date, admdvs,
        **kwargs):
    """
    运行卡聚集检测
    Parameters:
    --------------------------------
    db_type: str, 数据库类型，目前仅支持clickhouse和hive
    db_login_info: dict, 数据库登录信息
    sql: sql
    start_date: str, 开始日期，格式为“yyyy-mm-dd”
    end_date: str, 结束日期，格式为“yyyy-mm-dd”
    admdvs: str or None, 医保区划
    **kwargs: help(MultiCardDetection).

    Return:
    --------------------------------
    result: pandas.DataFrame
    """
    # 从数据库获取数据
    db_operator = DBOperator(db_type, db_login_info)
    df = db_operator.read_sql(sql)
    logger.info('columns: {}'.format(df.columns))
    logger.info('shape: {}'.format(df.shape))
    if df.shape[0] == 0:
        return None
    # 卡聚集检测
    mcd = MultiCardDetection(**kwargs)
    result = mcd.run(df)
    if result is not None:
        result['input_begndate'] = start_date
        result['input_enddate'] = end_date
        result['input_admdvs'] = admdvs
        # group_id前添加时间戳
        result['group_id'] = str(int(time.time())) + '_' + result['group_id']
    return result


class MultiCardDetection:
    """
    卡聚集检测
    """
    def __init__(self,
                 time_interval,
                 min_count, min_size, max_size, min_jg_num,
                 min_person_ratio_in_subgroup, 
                 min_risk_clinic_ratio_in_group,
                 resolution_parameter=10, n_jobs=1):
        """
        指定时间段内进行卡聚集检测
        Parameters:
        --------------------------------
        time_interval: int，时间间隔（秒）
        min_count: int, 最小同时出现次数
        min_size: int, 社区最小尺度
        max_size: int, 社区最大尺度
        min_jg_num: int, 至少涉及机构数
        min_person_ratio_in_subgroup: float, 风险子组人数占比最小值
        min_risk_clinic_ratio_in_group: float, 组内风险就诊人次占比最小值
        resolution_parameter: the resolution parameter to use.
        Higher resolutions lead to more smaller communities, while 
        lower resolutions lead to fewer larger communities.
        n_jobs: int, 进程数
        """
        self.time_interval = time_interval
        self.min_count = min_count
        self.min_size = min_size
        self.max_size = max_size
        self.min_jg_num = min_jg_num
        self.min_person_ratio_in_subgroup = min_person_ratio_in_subgroup
        self.min_risk_clinic_ratio_in_group = min_risk_clinic_ratio_in_group
        self.resolution_parameter = resolution_parameter
        self.n_jobs = n_jobs

    @staticmethod
    def get_risk_pairs(
            df, time_interval, 
            min_count=0, min_jg_num=0, n_jobs=1):
        """
        输入数据，获取卡聚集风险对
        Parameters:
        ----------------------------------------------
        df: pandas.DataFrame
        time_interval: int, 时间间隔
        min_count: int, 最小同时出现次数
        min_jg_num: int, 至少涉及机构数
        n_jobs: int, 进程数

        Returns:
        -----------------------------------------------
        result: pandas.DataFrame
        """
        # 每人就诊次数必须大于或等于min_count
        jz_counts = df['person_id'].value_counts()
        jz_counts = jz_counts[jz_counts >= min_count]
        df = df[df['person_id'].isin(jz_counts.index)]
        del jz_counts

        # 数据按person_id分批，每次输入一批数据与完整数据对比，获取风险对
        person_ids = sorted(df['person_id'].unique())
        batch_size = int(1e10 * len(person_ids) / df.shape[0]**2)
        num_batches = (len(person_ids) - 1) // batch_size + 1
        n_jobs = min(n_jobs, num_batches)
        if n_jobs > 1:
            risk_pairs = Parallel(n_jobs=n_jobs, verbose=50)(
                delayed(_get_risk_pairs)(
                    df.loc[
                        df['person_id'].isin(person_ids[batch_size*i:batch_size*(i+1)]), 
                        ['med_type', 'flx_med_org_id', 'person_id', 'adm_time', 'adm_time_win']
                    ], 
                    df.loc[
                        df['person_id'] >= min(person_ids[batch_size*i:batch_size*(i+1)]),
                        ['med_type', 'flx_med_org_id', 'person_id', 'adm_time', 'adm_time_win']
                    ], 
                    time_interval, 
                    min_count=min_count, 
                    min_jg_num=min_jg_num
                )
                for i in range(num_batches)
            )
        else:
            risk_pairs = [
                _get_risk_pairs(
                    df.loc[
                        df['person_id'].isin(person_ids[batch_size*i:batch_size*(i+1)]), 
                        ['med_type', 'flx_med_org_id', 'person_id', 'adm_time', 'adm_time_win']
                    ], 
                    df.loc[
                        df['person_id'] >= min(person_ids[batch_size*i:batch_size*(i+1)]),
                        ['med_type', 'flx_med_org_id', 'person_id', 'adm_time', 'adm_time_win']
                    ], 
                    time_interval, 
                    min_count=min_count, 
                    min_jg_num=min_jg_num
                )
                for i in range(num_batches)
            ]

        risk_pairs = pd.concat(risk_pairs)
        return risk_pairs

    @staticmethod    
    def build_graphs(data, risk_pairs):
        """
        卡聚集建图
        Parameters:
        -----------------------------
        data: pandas.DataFrame
        risk_pairs: pandas.DataFrame, 卡聚集风险对

        Returns:
        ----------------------------------
        graph: igraph.Graph
        graph2: igraph.Graph
        """
        item_list = list(set(j for i in risk_pairs.index for j in i))
        logger.info('nodes: {}'.format(len(item_list)))
        pairs = risk_pairs.index
        pairs_cnt = list(risk_pairs['jzcs'])
        logger.info('edges: {}'.format(len(pairs)))

        # 建图1  
        graph = ig.Graph()
        graph.add_vertices(item_list)
        graph.add_edges(pairs)
        graph.es['weight'] = pairs_cnt
        logger.info('Graph1: nodes {}, edges {}'.format(
            graph.vcount(), graph.ecount()))

        temp = data[data['person_id'].isin(item_list)][
            ['flx_med_org_id', 'adm_date', 'person_id']]
        # 建图2
        graph2 = ig.Graph()
        for i in item_list:
            graph2.add_vertex(i, type='person')
        for i in temp['flx_med_org_id'].unique():
            graph2.add_vertex(str(i), type='jg')
        for i in temp['adm_date'].unique():
            graph2.add_vertex(str(i), type='time')   
        edges_dict = {}
        for row in temp.values:
            jg = str(row[0])
            t = str(row[1])
            item = str(row[2])
            if (item, jg) in edges_dict:
                edges_dict[(item, jg)] += 1
            else:
                edges_dict[(item, jg)] = 1
            if (t, item) in edges_dict:
                edges_dict[(t, item)] += 1
            else:
                edges_dict[(t, item)] = 1
        graph2.add_edges(edges_dict.keys())
        graph2.es['weight'] = edges_dict.values()
        logger.info('Graph2: nodes {}, edges {}'.format(
            graph2.vcount(), graph2.ecount()))
        return graph, graph2

    @staticmethod
    def community_pruning(subgraph, min_count, min_size, min_jg_num):
        """
        社区剪枝
        :param subgraph: igraph.Graph, 图
        :param min_count: int, 最小同时出现次数
        :param min_size: int, 连通分量最小尺度
        :param min_jg_num: int, 至少涉及机构数
        """
        person_num = len(subgraph.vs.select(type='person'))
        t_num = len(subgraph.vs.select(type='time'))
        jg_num = len(subgraph.vs.select(type='jg'))
        if t_num < min_count or jg_num < min_jg_num or person_num < min_size:
            return None
        # Loop：删除度过少的时间和机构，删除时间数过少的个人
        for i in range(1, 11):
            last_vcount = subgraph.vcount()
            # 删除时间数过少的个人
            ts = set(subgraph.vs.select(type='time')['name'])
            subgraph.delete_vertices(
                [node['name'] for node in subgraph.vs.select(type='person') 
                 if len(set(subgraph.vs[subgraph.neighbors(node)]['name']) & ts) < max(
                    min_count, len(ts)*0.05*i)]
            )
            # 删除度过少的时间和机构
            m = max(2, len(subgraph.vs.select(type='person')) * 0.05 * i)
            subgraph.vs.select(type='jg', _degree_lt=m).delete()
            subgraph.vs.select(type='time', _degree_lt=m).delete()
            if subgraph.vcount() == last_vcount:
                break

        persons = subgraph.vs.select(type='person')
        ts = subgraph.vs.select(type='time')
        jgs = subgraph.vs.select(type='jg')
        if len(ts) < min_count or len(jgs) < min_jg_num or len(persons) < min_size:
            return None

        degree1 = np.mean(ts.degree())
        degree2 = np.mean(jgs.degree())
        degree3 = np.mean(persons.degree())
        result = {
            'c_times': ts['name'],
            'c_jgids': jgs['name'],
            'c_person_ids': persons['name'],
            'size': len(persons),
            'degree1': degree1,
            'degree2': degree2,
            'degree3': degree3
        }
        return result

    def detect_multicards(self, graph, graph2):
        """
        卡聚集函数
        :param graph: igraph.Graph，卡聚集图1
        :param graph2: igraph.Graph，卡聚集图2
        """
        time1 = time.time()
        # 社区
        communities = community_leiden(
            graph,
            'modularity', 
            weights='weight', 
            resolution_parameter=self.resolution_parameter*2, 
            n_iterations=300)
        communities = [x for x in communities if len(x) >= self.min_size]
        logger.info('社区数(粗分): {}'.format(len(communities)))    

        # 分解大社区
        temp1 = [x for x in communities if len(x) > self.max_size]
        communities = [x for x in communities if len(x) <= self.max_size]       
        while len(temp1) > 0:
            if self.n_jobs <= 1 or len(temp1) <= 20:
                temp2 = []
                for x in temp1:
                    temp3 = community_leiden(
                        graph.subgraph(x),
                        'modularity', 
                        weights='weight', 
                        resolution_parameter=self.resolution_parameter, 
                        n_iterations=300)
                    temp2.extend([x for x in temp3 if len(x) >= self.min_size])
                    del temp3
            else:
                with Parallel(
                        n_jobs=min(self.n_jobs, len(temp1)//20), 
                        batch_size=20, 
                        pre_dispatch='40*n_jobs', 
                        verbose=50) as parallel:
                    temp2 = parallel(
                        delayed(community_leiden)(
                            graph.subgraph(x),
                            'modularity', 
                            weights='weight', 
                            resolution_parameter=self.resolution_parameter, 
                            n_iterations=300
                        ) for x in temp1
                    )
                temp2 = [x for x1 in temp2 for x in x1 if len(x) >= self.min_size]

            temp1 = [x for x in temp2 if len(x) > self.max_size]
            communities.extend([x for x in temp2 if len(x) <= self.max_size])
            del temp2

        # del temp1
        logger.info('社区数(细分): {}'.format(len(communities)))
        logger.info('社区长度分布：{}'.format(
            sorted(Counter(list(map(len, communities))).items(),
                   key=lambda x: x[0], reverse=True)))

        # 社区剪枝
        result = []
        for x in communities:
            # 取出个人相关时间和机构
            neighbors = list(set(j for i in x for j in graph2.neighbors(i)))
            neighbors = list(graph2.vs[neighbors]['name'])
            subgraph2 = graph2.subgraph(x+neighbors)
            # ig.summary(subgraph2)
            temp = self.community_pruning(
                subgraph2, 
                min_count=self.min_count, 
                min_size=self.min_size, 
                min_jg_num=self.min_jg_num)
            if temp:
                result.append(temp)

        # 风险分
        for r in result:
            subgraph2 = graph.subgraph(r['c_person_ids'])
            r['connectivity'] = subgraph2.is_connected()
            r['degree4'] = np.mean(subgraph2.degree())
        result = pd.DataFrame(result)

        def cal_score(x):
            y = x['connectivity'] \
                + (x['degree1'] + x['degree2'] + x['degree3']) * 0.1 \
                + x['degree4'] / x['size']
            return y

        if len(result) > 0:
            result['score'] = result.apply(cal_score, axis=1)
            result.sort_values(['score'], ascending=False, inplace=True)
            result = result.iloc[:10000]
        print('结果：\n{}'.format(result))
        logger.info('elapse {:.0f}s'.format(time.time()-time1))
        return result 

    def convert_result_to_long(self, result):
        """
        将结果转换为长表：一行拆多行
        """
        result2 = []
        i = 0
        for _, row in result.iterrows():
            i += 1
            temp2 = row.to_dict()
            c_times = temp2.pop('c_times')
            c_jgids = temp2.pop('c_jgids')
            c_person_ids = temp2.pop('c_person_ids')
            for j1 in c_times:
                for j2 in c_jgids:
                    for j3 in c_person_ids:
                        temp3 = temp2.copy()
                        temp3['adm_date'] = j1
                        temp3['flx_med_org_id'] = j2
                        temp3['person_id'] = j3
                        result2.append(temp3)
        result2 = pd.DataFrame(result2)
        result2['adm_date'] = result2['adm_date'].astype(str)
        result2['person_id'] = result2['person_id'].astype(str)
        return result2  

    def filter_risk_groups(self, result):
        """
        风险组筛选
        """
        result3 = []
        for _, group in result.groupby('group_id'):
            group_persons_num = group['person_id'].nunique()
            subgroups = []
            subgroup_id = 0
            risk_clinic_ratio = 0
            for _, subgroup in group.groupby(['flx_med_org_id', 'adm_date']):
                # 风险子组人数占比大于阈值
                person_ratio = subgroup['person_id'].nunique()/group_persons_num
                if person_ratio >= self.min_person_ratio_in_subgroup:
                    subgroup_id += 1
                    subgroup = subgroup.copy()
                    subgroup['subgroup_id'] = str(subgroup_id)
                    subgroups.append(subgroup)
                    risk_clinic_ratio += subgroup.shape[0]
            risk_clinic_ratio /= group.shape[0]
            # 风险就诊人次占比大于阈值
            if risk_clinic_ratio >= self.min_risk_clinic_ratio_in_group:
                subgroups = pd.concat(subgroups)
                subgroups['risk_clinic_ratio'] = risk_clinic_ratio
                result3.append(subgroups)
        if result3:
            result3 = pd.concat(result3)
            result3 = result3[[
                'group_id', 'risk_clinic_ratio', 'subgroup_id', 
                'person_id', 'med_clinic_id',  
                'flx_med_org_id', 'med_type', 'adm_date', 'adm_time'
            ]]     
        else:
            result3 = None
        print('调整后结果：\n{}'.format(result3))
        return result3

    def run(self, df):
        """
        输入数据，运行卡聚集检测
        Parameters:
        --------------------------------
        df: pandas.DataFrame, input data，with columns: [
            'admdvs','med_clinic_id','person_id','med_type','flx_med_org_id',
            'adm_time', 'adm_date']
        """
        # 时间窗口编号
        if 'adm_time_win' not in df.columns:
            try:
                df['adm_time_win'] = (
                    df['adm_time'].view('int') // 1000000000) // self.time_interval
            except Exception:
                df['adm_time'] = pd.to_datetime(df['adm_time'])
                df['adm_time_win'] = (
                    df['adm_time'].view('int') // 1000000000) // self.time_interval
        
        df['adm_date'] = df['adm_date'].astype('str')

        # 获取卡聚集风险对
        risk_pairs = self.get_risk_pairs(
            df, 
            self.time_interval, 
            min_count=self.min_count, 
            min_jg_num=self.min_jg_num, 
            n_jobs=self.n_jobs
        )

        # 建图
        graph, graph2 = self.build_graphs(df, risk_pairs)
        del risk_pairs

        # 卡聚集检测
        result = self.detect_multicards(graph, graph2)
        del graph, graph2
        
        if len(result) == 0:
            return None

        # 调整结果
        result['group_id'] = list(map(str, range(1, len(result)+1)))
        result = result[['group_id', 'c_times', 'c_jgids', 'c_person_ids']]

        # 转换为长表：一行拆多行
        result = self.convert_result_to_long(result)
        # 关联挂号表
        temp = df[df['person_id'].isin(result['person_id'].unique())]
        temp = temp[
            temp['flx_med_org_id'].isin(result['flx_med_org_id'].unique())]
        result['adm_date'] = result['adm_date'].astype(str)
        result['person_id'] = result['person_id'].astype(str)
        result = result.merge(
            temp, 
            how='inner', 
            on=['person_id', 'flx_med_org_id', 'adm_date'])

        # 风险组筛选
        result = self.filter_risk_groups(result)
        return result


def _get_risk_pairs(
        df_batch, df_all, time_interval, 
        min_count=0, min_jg_num=0):
    """
    输入数据，获取卡聚集风险对
    Parameters:
    ----------------------------------------------
    df_batch: pandas.DataFrame, df_all的一部分
    df_all: pandas.DataFrame
    time_interval: int, 时间间隔
    min_count: int, 最小同时出现次数
    min_jg_num: int, 至少涉及机构数

    Returns:
    -----------------------------------------------
    result: pandas.DataFrame
    """
    # 相同时间窗口直接merge
    df_pairs = df_batch.merge(
        df_all, 
        how='inner', 
        on=['med_type', 'flx_med_org_id', 'adm_time_win']
    )[['person_id_x', 'person_id_y', 'flx_med_org_id']]
    df_pairs = df_pairs[df_pairs['person_id_x'] < df_pairs['person_id_y']]
    
    # 不同时间窗口但时间差小于阈值
    # 左边时间窗口小1
    df_batch['adm_time_win'] = df_batch['adm_time_win'] + 1
    temp = df_batch.merge(
        df_all, 
        how='inner', 
        on=['med_type', 'flx_med_org_id', 'adm_time_win'])
    temp = temp.loc[
        (temp['person_id_x'] < temp['person_id_y']) & (
            temp['adm_time_y']-temp['adm_time_x']).map(
                lambda x: x.seconds < time_interval),
        ['person_id_x', 'person_id_y', 'flx_med_org_id']
    ]
    df_pairs = pd.concat([df_pairs, temp], ignore_index=True)
    del temp
    
    # 左边时间窗口大1（注：因前面加了1，所以这里要减2）
    df_batch['adm_time_win'] = df_batch['adm_time_win'] - 2
    temp = df_batch.merge(
        df_all, 
        how='inner', 
        on=['med_type', 'flx_med_org_id', 'adm_time_win'])
    temp = temp.loc[
        (temp['person_id_x'] < temp['person_id_y']) & (
            temp['adm_time_x']-temp['adm_time_y']).map(
                lambda x: x.seconds < time_interval),
        ['person_id_x', 'person_id_y', 'flx_med_org_id']
    ]
    df_pairs = pd.concat([df_pairs, temp], ignore_index=True)
    del temp
    
    result = df_pairs.groupby(['person_id_x', 'person_id_y']).agg(
        jzcs=pd.NamedAgg('flx_med_org_id', 'count'),
        jg_num=pd.NamedAgg('flx_med_org_id', 'nunique')
    )
    
    if min_count and min_jg_num:
        result = result[
            (result['jzcs'] >= min_count) & (result['jg_num'] >= min_jg_num)]
    elif min_count:
        result = result[result['jzcs'] >= min_count]
    elif min_jg_num:
        result = result[result['jg_num'] >= min_jg_num]
    return result


def community_leiden(graph, *args, **kwargs):
    """
    在图上使用leiden算法获得社区，返回各社区各成员的name
    """
    result = graph.community_leiden(*args, **kwargs)
    result = [graph.vs[x]['name'] for x in result]
    return result