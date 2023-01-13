import os
import re

base_url = "/root/workshop32/dataset/"
util_url = "/root/workshop32/dataset/util/"
set_name = "large_set"  # large_set or small_set

find_word = {}

def fileSnippet(filepath, filename):
    print(filename)
    fSnippet = {'_total': 0, '_filename': filename, '_snippet': ""}
    i = 0
    with open(filepath, encoding='utf8') as f:
        for line in f.readlines():
            line = re.sub(' +', ' ', line)
            if i < 10: # get 10 lines as snippet
                snippet = line.replace(":", "").replace("\n", "")
                fSnippet['_snippet'] += ' ' + snippet
                i += 1
            else:
            	 break;

    return fSnippet


def list_to_str(list):
    ans = "["
    for i in range(len(list)):
        if i != (len(list) - 1):
            ans = ans + list[i] + ','
        else:
            ans = ans + list[i] + ']\n'

    return ans


if __name__ == '__main__':
    words_in_file = {}
    file_snippet_all = []

    data_filenames = os.listdir(base_url + set_name)
    #print(data_filenames)

    for data_filename in data_filenames:
        file_full_path = base_url + set_name + "/" + data_filename
        file_snippet = fileSnippet(file_full_path, data_filename)
        file_snippet_all.append(file_snippet)

    with open(util_url + set_name + "/" + "file_snippet.txt", 'w',
              encoding='utf8') as f:
        for file_snippet in file_snippet_all:
            line = file_snippet['_filename'] + ":" + str(file_snippet['_snippet'])
            print(line)
            f.write(line + '\n')

