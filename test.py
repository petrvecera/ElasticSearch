import elastic_search

if __name__ == '__main__':
    elastic_search.load('test.json')
    r = elastic_search.elastic.simple_search('project', 'ad', 'user:aaa')
    print(r.text)
