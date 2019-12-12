import re
import jieba
import pickle
from tools.configreader import online_config

words_dict_path = online_config.get('Path','words_dict_path')
jieba.load_userdict(words_dict_path)


class Judge():
    def __init__(self):
        self.mapfile = online_config.get('Path','mapfile')
        self.stopwords_path = online_config.get('Path','stopwords_path')
        self.mapdict = self.load_map(self.mapfile)
        self.stopwords = self.load_stopwords(self.stopwords_path)

        self.question_sign = ['?','？','吗','嘛','么','啊','呀']
        self.this_sign = ['这是','这款','这条','这件','这个']
        self.pair_sign = ['搭配','怎么搭','怎么配','搭什么','配什么']
        self.stock_sign = ['缺货','无货','有货','现货','补货','没货','什么时候有','什么时候补']
        self.have_sign = ['有吗','有嘛','有没有','有么','有不','没有嘛','没有吗']
        self.which_sign = ['哪件','哪款','哪个','哪条','那件','那款','那个','那条']
        self.recom_sign = ['推荐']
        self.uncertain_sign = ['?','？','吗','嘛','么','哪件','哪款','哪个','哪条','什么','那件','那款','那个','那条']
        self.have_pattern = re.compile(r'有.*[吗嘛么]')
        self.yes_pattern = re.compile(r'是.*[吗嘛么]')
        self.can_pattern = re.compile(r'可以.*[吗嘛么]')
        self.can_pattern2 = re.compile(r'能.*[吗嘛么]')
        self.patterns = [self.have_pattern,self.yes_pattern,self.can_pattern,self.can_pattern2]

    def load_map(self,mapfile):
        with open(mapfile,'rb') as f:
            mapdict = pickle.load(f)
        return mapdict

    def load_stopwords(self,filepath):
        with open(filepath, 'r', encoding='utf8') as f:
            lines = f.readlines()
        stop_words = [(i.strip(), 1) for i in lines]
        stop_words_dict = dict(stop_words)
        return stop_words_dict

    def pattern_match(self, text):
        status = 0
        for pattern in self.patterns:
            if pattern.findall(text):
                status = 1
                break
        return status

    def this_match(self, text):
        status = 0
        for this_word in self.this_sign:
            if status:
                break
            if this_word in text:
                status = 1
        return status

    def pair_match(self, text):
        status = 0
        for pair_word in self.pair_sign:
            if status:
                break
            if pair_word in text:
                for uncertain_word in self.uncertain_sign:
                    if uncertain_word in text:
                        status = 1
                        break
        return status

    def stock_match(self, text):
        status = 0
        if '没有' in text and '有没有' not in text:
            status = 1

        for stock_word in self.stock_sign:
            if status:
                break
            if stock_word in text:
                status = 1
        return status

    def recom_match(self, text):
        status = 0
        if '推荐' in text:
            status = 1

        for have_word in self.have_sign:
            if status:
                break
            if have_word in text:
                status = 1

        for which_word in self.which_sign:
            if status:
                break
            if which_word in text:
                status = 1

        if not status:
            status = self.pattern_match(text)

        return status

    def check_category(self, tags: dict):
        status = 0
        if 'sub_category' in tags:
            status = 1
        return status

    def cut(self, text):
        words = list(jieba.cut(text))
        words = [word for word in words if not self.stopwords.get(word,None)]
        return words

    def maper(self, text):
        words = self.cut(text)
        res = {}

        for word in words:
            map_word = self.mapdict.get(word,'')
            if map_word:
                transform_map = self.assist_maper(map_word)
                if res.get(map_word[1],None):
                    res[map_word[1]] = res[map_word[1]]+' '+map_word[0]
                else:
                    res[map_word[1]] = map_word[0]

                if transform_map:
                    if res.get(transform_map[1], None):
                        res[transform_map[1]] = res[transform_map[1]]+' '+transform_map[0]
                    else:
                        res[transform_map[1]] = transform_map[0]

        return res

    def assist_maper(self,map_word):
        transform_map = []
        key = 'sub_category'
        if map_word[1] == 'stereotype':
            if map_word[0].endswith('裤'):
                transform_map = ['裤子',key]
            if '外套' in map_word[0]:
                transform_map = ['短外套', key]
            if map_word[0].endswith('鞋'):
                transform_map = ['鞋子', key]
            if map_word[0].endswith('裙'):
                transform_map = ['裙子', key]
            if map_word[0].endswith('帽'):
                transform_map = ['帽子', key]
            if map_word[0].endswith('靴'):
                transform_map = ['靴子', key]
            if map_word[0] == '短袖':
                transform_map = ['T恤',key]
        return transform_map

    def scener(self, text):
        """
        scene_type:0:不推荐,1:搭配,2:无货推荐,3:咨询推荐
        """
        scene_type = 0
        pair_status = self.pair_match(text)
        stock_status = self.stock_match(text)
        recom_status = self.recom_match(text)

        if pair_status and not scene_type:
            scene_type = 1

        if stock_status and not scene_type:
            scene_type = 2

        if recom_status and not scene_type:
            scene_type = 3

        return scene_type

    def tager(self, text,context=None):
        tags = self.maper(text)

        if context:
            for key in context:
                if tags.get(key,None):
                    tags[key] = tags[key]+' '+context[key]
                else:
                    tags[key] = context[key]

        return tags

    def judger(self, text,context=None):
        res = {}
        scene_type = self.scener(text)
        tags = {}
        if scene_type:
            tags = self.tager(text,context)

        if not tags:
            scene_type = 0
        res['scene_type'] = scene_type
        res['tags'] = tags
        return res


Judger = Judge()


if __name__ == '__main__':
    t = Judger.judger(text='有没有黑色的裤子',context={'sub_category':'打底裤','color':'红色','waist_style':'高腰'})
    print(t)

