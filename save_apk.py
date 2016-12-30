# -*- coding: utf-8 -*-
import json
import os
import sys
import util
reload(sys)
sys.setdefaultencoding('utf-8')

class SaveApk:
    def __init__(self):
        self.logger = util.init_log("log", "save_apk_log")
        self.conn = util.init_db_conn()
        self.cur = self.conn.cursor()
        self.attr_data = ["MD5", "name", "size", "pkg_name", "source", "category", "rating", "upload_time",
                          "download_time", "download_count", "detail_url", "new", "backup", "icon_path", "download_urls"]

    def load_from_file(self, path):
        result_list = []
        with open(path, "r") as fp:
            for index, line in enumerate(fp):
                result_list.append(line.replace("\n", ""))
        return result_list

    def get_attr(self, msg):
        tmp = []
        msg_1 = None
        try:
            msg_1 = msg.replace("u'", "\"").replace("'", "\"")
            obj = json.loads(msg_1)
            for attr in self.attr_data:
                data = obj.get(attr, " ")
                if "download_urls" == attr:
                    tmp_1 = ""
                    for item in data:
                        tmp_1 += item.strip("u") + ", "
                    data = tmp_1
                elif "source" == attr:
                    data = data.replace("_", ".")
                tmp.append(data)
            return tmp
        except Exception as e:
            self.logger.error("get_attr() exception: {0}, {1}, ___________{2}".format(e, msg, msg_1))

    def save2mysql(self, val_list):
        sql = "insert into apkpool_apk_source(md5, name, size, pkg_name, source, category, rating, uploadtime, downloadtime, " \
              "downcount, detailurl, newapk, existed, icon, downloadurls) values(%s, %s, %s, %s, %s, %s, %s, %s, " \
              "%s, %s, %s, %s, %s, %s, %s)"
        try:
            result = self.cur.executemany(sql, val_list)
            print result
            self.conn.commit()
        except Exception as e:
            self.logger.error("save2mysql() exception: {0}".format(e))

    def lunch(self, base_path):
        files = os.listdir(base_path)
        for file_s in files:
            path = base_path + file_s
            if os.path.isfile(path):
                save_apk.work(base_path + file_s)
        self.close_connect()

    def close_connect(self):
        self.cur.close()
        self.conn.close()

    def work(self, path):
        info_list = self.load_from_file(path)
        print len(info_list)
        for x in xrange(0, len(info_list), 2000):
            attr_list = []
            for data in info_list[x: x+2000]:
                attr_data = self.get_attr(data)
                if None is not attr_data:
                    attr_list.append(attr_data)
            self.save2mysql(attr_list)

if __name__ == "__main__":
    save_apk = SaveApk()
    save_apk.work("crawl_result/store_oppomobile_com")
    # save_apk.lunch("crawl_result/")
