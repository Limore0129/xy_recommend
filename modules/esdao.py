from elasticsearch import Elasticsearch
from tools.configreader import online_config
import time
import re
from tools.cute_log import logger
from tqdm import tqdm


class ES():
    def __init__(self):
        self.es = self.build_es()
        self.doc_type = 'recommend'
        self.num_pattern = re.compile(r'\d+')
        self.mapping = {'properties':{
            "product_id":{'type':'keyword'},
            "index_name":{'type':'text','index':'not_analyzed'},
            "cid":{'type':'text','index':'not_analyzed'},
            "ext_pro_id":{'type':'text','index':'not_analyzed'},
            "age":{'type':'integer_range'},
            "season":{'type': 'text', 'analyzer': 'ik_max_word', 'search_analyzer': 'ik_max_word'},
            "thickness":{'type': 'text', 'analyzer': 'ik_max_word', 'search_analyzer': 'ik_max_word'},
            "color":{'type': 'text', 'analyzer': 'ik_max_word', 'search_analyzer': 'ik_max_word'},
            "facing_material":{'type': 'text', 'analyzer': 'ik_max_word', 'search_analyzer': 'ik_max_word'},
            "sleeves_length":{'type': 'text', 'analyzer': 'ik_max_word', 'search_analyzer': 'ik_max_word'},
            "material":{'type': 'text', 'analyzer': 'ik_max_word', 'search_analyzer': 'ik_max_word'},
            "skirt_length":{'type': 'text', 'analyzer': 'ik_max_word', 'search_analyzer': 'ik_max_word'},
            "pants_length":{'type': 'text', 'analyzer': 'ik_max_word', 'search_analyzer': 'ik_max_word'},
            "waist_style":{'type': 'text', 'analyzer': 'ik_max_word', 'search_analyzer': 'ik_max_word'},
            "stereotype":{'type': 'text', 'analyzer': 'ik_max_word', 'search_analyzer': 'ik_max_word'},
            "clothe_length":{'type': 'text', 'analyzer': 'ik_max_word', 'search_analyzer': 'ik_max_word'},
            "mianliao":{'type': 'text', 'analyzer': 'ik_max_word', 'search_analyzer': 'ik_max_word'},
            "functions":{'type': 'text', 'analyzer': 'ik_max_word', 'search_analyzer': 'ik_max_word'},
            "hqt_style":{'type': 'text', 'analyzer': 'ik_max_word', 'search_analyzer': 'ik_max_word'},
            "sub_category":{'type': 'text', 'analyzer': 'ik_max_word', 'search_analyzer': 'ik_max_word'},
            "commodity_consult_cnt": {'type': 'integer'},
            "commodity_buy_cnt": {'type': 'integer'},
            "commodity_convert_rate": {'type': 'float'},
            "shop_name":{'type':'text','index':'not_analyzed'},
            "name":{'type': 'text', 'analyzer': 'ik_max_word', 'search_analyzer': 'ik_max_word'},
            "item_sales_num_1m":{'type': 'integer'},
            "item_sales_num_3m": {'type': 'integer'},
            "item_sales_num_7d": {'type': 'integer'},
            "order_sales_num":{'type': 'integer'},
            "seller_iid_tids_num":{'type': 'integer'},
            "seller_tids_num":{'type': 'integer'},
            "joint_rate":{'type': 'float'},
            "commodity_popularity":{'type': 'float'},
            "create_time":{'type':'date','format':'yyyy-MM-dd'},
            "status":{'type': 'integer'}
        },
            'dynamic':'false'
        }

        self.pair_mapping = {'properties':{
            "iid":{'type':'text'},
            "product_list":{'type':'text'},
            "create_time": {'type': 'date', 'format': 'yyyy-MM-dd'},
            "conf":{'type':'text','index':'not_analyzed'},
            "lift": {'type': 'text', 'index': 'not_analyzed'}
        },
            'dynamic': 'false'
        }

    def build_es(self):
        port = 9200
        timeout = 30
        host = online_config.get('Account','online_es')
        auth_name = online_config.get('Account','online_name')
        auth_pwd = online_config.get('Account','online_pwd')
        es = Elasticsearch([host],http_auth = (auth_name,auth_pwd),port = port,use_ssl = False,timeout = timeout)
        return es

    # 新建索引
    def shop_index(self,index_name):
        index_name = index_name.lower()
        pair_index = index_name+'_pair'

        self.init_index(index_name,self.mapping)
        self.init_index(pair_index,self.pair_mapping)

    # 新建索引
    def init_index(self,index_name,mapping):
        if not self.es.indices.exists(index_name):
            self.es.indices.create(index=index_name, ignore=400)
            self.es.indices.put_mapping(index=index_name, doc_type=self.doc_type, body=mapping)
            logger.info('创建索引%s'%index_name)

    # 根据mapping新增字段
    def add_field(self,index_name,mapping,mode=1):
        index_name = self.select_index_name(index_name,mode)
        if self.es.indices.exists(index_name):
            self.es.indices.put_mapping(index=index_name, doc_type=self.doc_type, body=mapping)
            logger.info('新增字段%s'%str(list(mapping['properties'].keys())))

    # 插入数据
    def insert_data(self,index_name,data:list,mode = 1):
        index_name = self.select_index_name(index_name, mode)
        if self.es.indices.exists(index_name):
            date = time.strftime('%Y-%m-%d')
            if mode == 1:
                data = self.transform_data(data)

            for d in tqdm(data):
                d['create_time'] = date
                self.es.index(index=index_name,doc_type=self.doc_type,body=d)
            self.es.indices.refresh(index=index_name)

            logger.info("%s插入%s条数据"%(index_name,len(data)))
        else:
            logger.info("%s不存在，请先建立索引再进行数据插入" % index_name)

    def transform_data(self,data):
        # 处理age字段
        res = []
        for d in data:
            d = self.process_age(d)
            res.append(d)
        return res

    def process_age(self,d:dict):
        if 'age' in d:
            age = d.get('age', '')
            age_range = self.num_pattern.findall(age)
            if age and len(age_range) == 2:
                age_range.sort(key=lambda x:int(x))
                d['age'] = {'gte':int(age_range[0]),'lte':int(age_range[1])}
            else: d.pop('age')
        return d

    # 根据query进行查询
    # mode:0:搭配搜索,1:商品属性搜索,2:根据人气度进行排序
    def search(self,index_name,query,size=10,mode=1):
        index_name = self.select_index_name(index_name,mode)
        dsl = self.select_dsl(query,mode)
        hits = []

        if self.es.indices.exists(index=index_name):
            result = self.es.search(index=index_name,doc_type=self.doc_type,body=dsl,size = size)
            hits = result['hits']['hits']
        return hits

    # 根据query生成dsl，用于根据商品属性进行推荐
    def dsler(self,data):
        dsl = {}
        bool_json = {}
        should_list = []
        keys = list(data.keys())
        filter_json = {'match':{'status':1}}
        if 'sub_category' in keys:
            filter_json = [{'match':{'sub_category':data['sub_category']}},{'match':{'status':1}}]
            keys.remove('sub_category')
        bool_json['filter'] = filter_json

        common_fields = ["facing_material","sleeves_length","material","stereotype","clothe_length",
                         "mianliao","pants_length","hqt_style","name"]
        for key in keys:
            fields = []
            if key == 'age':
                age_res = self.num_pattern.findall(data[key])
                try:
                    age = int(age_res[0])
                except:
                    age = 0
                match_json = {'match': {key: age}}
            else:
                fields.extend(common_fields)
                fields.append(key)
                match_json = {'multi_match':{"query":data[key],"fields":fields}}
            should_list.append(match_json)

        bool_json['should'] = should_list
        dsl['query'] = {'bool':bool_json}
        # dsl['sort'] = {'commodity_popularity':{'order':'desc'}}

        return dsl

    # 选择dsl
    def select_dsl(self,query,mode):
        '''
        :param query:
        :param mode: 0为匹配，1为咨询相似，2为首句推荐
        :return:
        '''
        if mode==1:
            dsl = self.dsler(query)
        elif mode==2:
            dsl = {'query': {"match_all": {}},'sort':{'commodity_popularity':{'order':'desc'}}}
        else:
            dsl = {'query':{'match':query}}
        return dsl

    # 选择index
    def select_index_name(self,index_name,mode):
        if mode:
            index_name = index_name.lower()
        else:
            index_name = index_name.lower()+'_pair'
        return index_name

    # 删除索引
    def delete_index(self,index_name,mode=1):
        index_name = self.select_index_name(index_name,mode)
        if self.es.indices.exists(index_name):
            self.es.indices.delete(index_name, ignore=[400, 404])
            logger.info('已删除%s' % index_name)
        else:
            logger.info('%s不存在' % index_name)

    # 删除商品
    def delete_product(self,index_name,query_dict,mode=1):
        # query_dict = {'product_id':'ujdiejdi1w323'}
        index_name = self.select_index_name(index_name,mode)
        if self.es.indices.exists(index_name):
            dsl = {"query": {"match": query_dict}}
            self.es.delete_by_query(index=index_name, doc_type=self.doc_type, body=dsl, refresh=True)
            logger.info('删除成功')
        else:
            logger.info('%s不存在' % index_name)

    # 更新字段值
    def update_attr(self,index_name,query_dict,update_dict,mode=1):
        index_name = self.select_index_name(index_name,mode)
        key = list(update_dict.keys())[0]

        if self.es.indices.exists(index_name):
            dsl = {"script":
                {
                    "inline": "ctx._source.%s = params.%s"%(key,key),
                    "lang": "painless",
                    "params": update_dict
                },
                "query": {"match": query_dict}
            }
            self.es.update_by_query(index=index_name, doc_type=self.doc_type, body=dsl, refresh=True)
            logger.info('更新成功')
        else:
            logger.info('%s不存在' % index_name)

    # 获取索引库里的所有数据
    def get_all_data(self,index_name,mode=1):
        index_name = self.select_index_name(index_name, mode)
        dsl = {'query': {"match_all": {}}}
        hits = []

        if self.es.indices.exists(index=index_name):
            result = self.es.search(index=index_name, doc_type=self.doc_type, body=dsl)
            hits = result['hits']['hits']
        return hits

    # 获取所有索引
    def all_index(self):
        indices = self.es.cat.indices()
        indices_info = indices.split('\n')[:-1]
        index_names = [i.split(' ')[2] for i in indices_info]
        return index_names


if __name__ == '__main__':
    import pandas as pd
    import numpy as np

    # data_path = '../data/items.csv'
    # data = pd.read_csv(data_path)
    # ds = data.to_dict(orient='records')

    es = ES()
    test_mapping = {'properties':{
            "iid":{'type':'text'},
            "product_list":{'type':'text'},
        },
            'dynamic': False
        }
    test_index = 'test_dynamic'
    test_data = [{'iid':12345,'product_list':'87654','test_field':'test1'},{'iid':123456,'product_list':'987654','test_field':'test2'}]
    es.init_index(test_index,test_mapping)
    es.insert_data(test_index,test_data)
    print(es.get_all_data(test_index))
    es.delete_index(test_index,mode=1)
    # b = es.search(index_name='recom_test',query={},mode=2)
    # for i in b:
    #     print(i['_source']['commodity_popularity'])











