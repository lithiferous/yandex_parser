from bs4 import BeautifulSoup
from multiprocessing import Pool
from selenium import webdriver as w

import csv
import os
import pandas as pd
import pickle as pkl
import requests


def get_html(url):
    r = requests.get(url, timeout = 5)
    return r

def open_dict(dict_name):
    with open(dict_name, 'rb') as f:
        return pkl.load(f)

def close_dict(dict_name, obj):
    with open(dict_name, 'wb') as f:
        pkl.dump(obj, f, protocol=pkl.HIGHEST_PROTOCOL)

def get_url_list(url_filename):
    '''
    Returns a list of urls
    Input - csv/txt file, column with links
    '''
    df = pd.read_csv(url_filename, sep ='\n', header = None)
    url_list = list(filter(lambda a: a != 0, list(df.ix[:,0])))
    return url_list

def get_url_category(url):
    '''
    Returns a list of categories from URL
    '''
    r = get_html(url)
    page = BeautifulSoup(r.content, "lxml")
    page.find('catlinks', attrs={'class':"mw-normal-catlinks", 'id':"mw-normal-catlinks"})
    links = []
    for ul in page:
        for li in page.findAll('li'):
            if li.find('ul'):
                break
            links.append(li)
    links = page.findAll('a')
    cats = []
    for link in links:
        cats.append(link.get('title'))
    rep_str = 'Category:'
    f = lambda x: x and x.startswith(rep_str)
    list_cats = [cat.replace(rep_str, '') for cat in list(filter(f, list(set(cats))))]
    return list_cats

def update_dict_categories(filename, list_cats):
    '''
    Updates existing dict.pkl with list of categories
    '''
    dict_cats = open_dict(filename)
    dict_tmp = {}
    for k, v in dict_cats.items():
        for cat in range (0, len(list_cats)):
            if k == list_cats[cat]:
                v += 1
                dict_tmp.update([(k, v)])
            elif list_cats[cat] not in list(dict_cats.keys()):
                dict_tmp.update([(list_cats[cat], 1)])
    dict_cats.update(dict_tmp)
    close_dict(filename, dict_cats)
    print('Length after assigning cats:', len(dict_cats), '\n')
    return dict_cats


def process(list):
    '''
    Input list[url_path, dict_name]
    '''
    url_path = list[0]
    dict_name = list[1]

    url_list = get_url_list(url_path)
    for i in range(0, len(url_list)):
        list_cats = get_url_category(url_list[i])
        update_dict_categories(dict_name, list_cats)

def main():
    dict_name = 'dict_cats'
    splits = 4
    file_csv_path = '.\\Data\\url_Lists\\'

    url_path_list = []
    for i in range(splits):
        url_path_list.append(os.path.abspath(file_csv_path+'url_list_{}.csv'.format(i+1)))
        print(url_path_list[i])

    dict_path_list = []
    for i in range(splits):
        dict_path_list.append(os.path.abspath(file_csv_path+'dict_cats_{}.pkl'.format(i+1)))
        print(dict_path_list[i])

    files = []
    for i in range(splits):
            files.append([url_path_list[i],dict_path_list[i]])
            print(files[i])

    pool = Pool(processes=4)
    pool.map(process, files)

if __name__ == '__main__':
    main()
