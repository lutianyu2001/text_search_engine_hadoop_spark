from pyspark import SparkConf, SparkContext
import os, re, time, math
from pyspark.sql.session import SparkSession

# Create your views here.
from django.shortcuts import redirect
from django.http import HttpResponse
from django.template import loader

#base_url = "/root/workshop32/dataset/"

hdfs_url = r'hdfs://workshop32master:9000/workshop32/dataset/'
util_url = "/root/workshop32/dataset/util/"

tfidf_index = "tfidf_index.txt" # filename for tfidf ranked index file
file_snippet = "file_snippet.txt"  # filename for snippet file

set_name = "large_set"

spark = SparkSession.builder.master('spark://workshop32master:7077').appName('MyApp').getOrCreate()

def index(request):
    if request.method == "POST":
        return HttpResponse(status=403)
    template = loader.get_template('search/index.html')
    context = {}
    return HttpResponse(template.render(context, request))


def search(request):
    if request.method == "POST":
        return HttpResponse(status=403)
    req = request.GET
    if not req.__contains__('word'):
        return redirect("/search/")

    search = req['word']
    files_attr = {}

    get_tf_idf(search, files_attr)
    get_snippet(files_attr)

    template = loader.get_template('search/search.html')
    file_list = attr_to_list(files_attr)

    context = {}
    if len(file_list) != 0:
        context['file_list'] = file_list

    return HttpResponse(template.render(context, request))


def detail(request):
    if request.method == "POST":
        return HttpResponse(status=403)
    req = request.GET
    if not req.__contains__('filename'):
        return HttpResponse(status=403)
    filename = req['filename']
    template = loader.get_template('search/detail.html')
    context = {'filename': filename}
    txt = ""
    
    f_rdd = spark.sparkContext.textFile(hdfs_url + set_name + '/' + filename)
    txt = '\n'.join(f_rdd.collect())
    #with open(base_url + set_name + '/' + filename, encoding='utf8') as f:
        #txt = f.read()
    
    context['txt'] = txt

    return HttpResponse(template.render(context, request))


def get_snippet(files_attr):
    with open(util_url + set_name + "/" + file_snippet, encoding='utf8') as f:
        for line in f.readlines():
            items = line.split(':')
            filename = items[0]
            snippet = items[1]
            if files_attr.__contains__(filename):
                files_attr[filename]["snippet"] = snippet


def get_tf_idf(search, files_attr):
    arr = ""
    with open(util_url + set_name + "/" + tfidf_index, encoding='utf8') as f:
        for line in f.readlines():
            items = line.split(':')
            word = items[0]
            if word == search:
                arr = items[1]
                break
    if arr == "":
        return []
    arr = arr.replace("[", "").replace("]", "").replace("\n", "")
    filenames = arr.split(",")
    for filename in filenames:
        filename = filename.replace(")", "")
        ff = filename.split("(")  # ff[0] is filename, ff[1] is count of the word of the file
        fn = ff[0]
        tfidf = float(ff[1])
        # print(fn)
        # print(wc)
        file_attr = {"tfidf": tfidf}
        files_attr[fn] = file_attr


def attr_to_list(files_attr):
    file_list = []
    for filename in files_attr.keys():
        item = {'filename': filename, 'snippet': files_attr[filename]['snippet']}
        file_list.append(item)
    return file_list
