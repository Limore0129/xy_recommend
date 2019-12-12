from modules.esdao import ES
from modules.judge import Judger
import pprint
from tools.warningBot import *
es = ES()


class Recommend():
    def __init__(self):
        pass

    def recom_by_question(self,shop_id,unit_key,question,scene_type,size=20,context=None,black_list=None):
        '''
        :param shop_id:
        :param question:
        :param scene_type: 1:搭配，2：无货，3：咨询选购
        :param size:
        :param context:
        :param black_list:
        :return:
        '''
        tags = Judger.tager(question,context)

        res = []
        results = []
        # scene_type:推搭配
        if scene_type == 1:
            pprint.pprint(tags)
            if "product_id" in tags:
                res = es.search(shop_id,unit_key,{"iid":tags["product_id"]},size=size,mode=0)
                return self.match_res(res,black_list)
        # scene_type:选购推荐
        if scene_type > 1:
            if 'sub_category' in tags:
                res = es.search(shop_id,unit_key,tags,size=size,mode=1)
        if res:
            for item in res:
                if item['_source']['ext_pro_id'] not in black_list:
                    results.append({"productId":item['_source']['ext_pro_id'],"productWeight":round(item['_score'],2)})
        return results

    def recom_by_node(self,shop_id,unit_key,scene_type,product=None,size=20,black_list=None):
        '''
        :param shop_id:
        :param product_id:
        :param scene_type: 4为首句推荐，5为订单挽回，6为取消订单，7为付款成功
        :param size:
        :param context:
        :param black_list:
        :return:
        '''
        res = []
        results = []
        if scene_type == 4:
            res = es.search(shop_id,unit_key,query={},size=size,mode=2)
            pprint.pprint(res)
        elif scene_type in [5,6]:
            if product.get('product_id',None) and product.get('attr',None):
                attr_dict = product.get('attr',None)
                res = es.search(shop_id,unit_key, attr_dict, size=size, mode=1)

        elif scene_type == 7:
            if product.get('product_id',None):
                res = es.search(shop_id,unit_key,{"iid":product.get('product_id',None)},size=size,mode=0)
            results = self.match_res(res,black_list)
            return results
        else:
            send_msg(msg = '推荐业务报警:\n'
                               'recom_by_node : wrong scene type,please check!')
        results  = self.common_res(res,black_list)
        return results

    def match_res(self,res,black_list):
        results = []
        if res:
            for item in res:
                if item['_score'] is None:
                    score = 0.00
                else:
                    score = item['_score']
                for prod in item['_source']['product_list'].split(' '):
                    if prod not in black_list and prod != '' and prod is not None:
                        results.append({"productId": prod, "productWeight": round(score, 2)})
        return results

    def common_res(self,res,black_list):
        results = []
        if res:
            for item in res:
                if item['_source']['ext_pro_id'] not in black_list  :
                    if item['_score'] is None: score = 0.00
                    else:  score = item['_score']
                    results.append({"productId": item['_source']['ext_pro_id'], "productWeight": round(score, 2)})
        return results

if __name__ == '__main__':
    r = Recommend()
    res = r.recom_by_question('recom_test','有没有红色的裤子',scene_type=2,black_list=[])
    pprint.pprint(res)


    # res = r.recom_by_node('recom_test',scene_type=4,black_list=[])
    # pprint.pprint(res)