import math
import os


base_url = "/root/workshop32/dataset"
util_url = "/root/workshop32/dataset/util"

index_file = "index_count.txt"  # index file contain word and display the count of word
file_count_file = "file_count.txt"  # count total word in a file

set_name = "small_set"


def take_tf_idf(dic):
    return dic['tf_idf']


def take_tf(dic):
    return dic['tf']


files_attr = {}


def index(word):
    arr = ""
    with open(util_url + "/" + set_name + "/" + index_file, encoding='utf8') as f:
        for line in f.readlines():
            words = line.split(':')
            if words[0] == word:
                arr = words[1]
                break
    if arr == "":
        return []
    arr = arr.replace("[", "").replace("]", "").replace("\n", "")
    filenames = arr.split(",")
    for filename in filenames:
        filename = filename.replace(")", "")
        ff = filename.split("(")  # ff[0] is filename, ff[1] is count of the word of the file
        fn = ff[0]
        wc = int(ff[1])
        # print(fn)
        # print(wc)
        file_attr = {"wordcount": wc}
        files_attr[fn] = file_attr

    return filenames


def total_count():
    with open(util_url + "/" + set_name + "/" + file_count_file, encoding='utf8') as f:
        for line in f.readlines():
            items = line.split(':')
            filename = items[0]
            words = int(items[1])
            if files_attr.__contains__(filename):
                files_attr[filename]["total_word"] = words


if __name__ == '__main__':
    files = os.listdir(base_url)
    total_file_num = len(files)

    search = 'item'

    index(search)
    total_count()

    # print(files_attr)

    contain_file_num = len(files_attr.keys())

    idf = math.log(total_file_num / (contain_file_num + 1))

    file_list = []

    for filename in files_attr.keys():
        files_attr[filename]['tf'] = files_attr[filename]['wordcount'] / files_attr[filename]["total_word"]
        files_attr[filename]['tf_idf'] = files_attr[filename]['tf'] * idf
        item = {'filename': filename, 'tf': files_attr[filename]['tf'], 'tf_idf': files_attr[filename]['tf_idf']}
        file_list.append(item)

    file_list.sort(key=take_tf, reverse=True)

    for f in file_list:
        print(f['filename'] + ":" + str(f['tf']))




