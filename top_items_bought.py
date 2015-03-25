from dteam import datastores
from datetime import date
ds = datastores.bi()
from dteam.api import apps

import json
import copy

def get_top_products(start_date, end_date):
    query = """
            select	cp.website_id, cp.pk_product_id, cp.title, cp.price_cents, w.identifier, w.primary_domain,
                    sum(cc.quantity) as cnt
            from	bizdw_v6.commerce_orders co
            join	bizdw_v6.commerce_carts cc
                on	co.pk_purchase_cart_id = cc.fk_purchase_cart_id
            join	bizdw_v6.commerce_products cp
                on	cc.fk_product_id = cp.pk_product_id
            join	bizdw_v6.websites w
                on	cp.website_id = w.object_id
            where	date(co.submitted_on) >= %s
                and date(co.submitted_on) <= %s
            group by cp.website_id, cp.pk_product_id
            order by cnt desc
            limit 10
            """
    
    print 'getting top 10 products ids & website ids..'
    
    with datastores.CursorFrom(ds.bi_pool, autocommit=True) as cursor:
        cursor.execute(query,(str(start_date),str(end_date)))
        data = cursor.fetchall()
    
    
    keys = ['website_id', 'product_id', 'item_title', 'item_price', 'identifier', 'primary_domain', 'num_purchased']
    results = []
    
    for row in data:
        temp_dict = dict(zip(keys, row))
        results.append(temp_dict)
    
    print "list of dicts size... " + str(len(data))
    
    return results


def get_data(session, url):
    resp = json.loads(session.get(url).content)
    data = resp.get('data', {})
    try:
        url = resp.get('next', 'no more')
        if 'page_size=50' in url:
            url = url.replace('page_size=50', 'page_size=1000')
    except:
        url = 'no more'
    return data, url


def get_store_url_id(session, url, store_id, LIMIT):
    counter = 0
    while url != 'no more' and counter < LIMIT:

        collections, url = get_data(session, url)

        #search json for product_id
        for collection_id, collection_item in collections.iteritems():
            if collection_id == store_id:
                return collection_item.get('urlId')
    
    return 'none'


def get_item_url_id(session, url, product_id, LIMIT):
    counter = 0
    while url != 'no more' and counter < LIMIT:

        collections, url = get_data(session, url)

        #search json for product_id
        for collection_id, collection_item in collections.iteritems():
            if collection_item.get('_id', 'none') == product_id:
                return collection_item.get('urlId'), collection_item.get('collectionId')
    
    return 'none', 'none'


def add_item_urls(data):
    session = apps.api()
    LIMIT = 100
    dict_list = []
    
    for item in data:
        # set endless urls        
        website_id = item.get('website_id')

        if website_id != None:
            url_item = "https://apps.squarespace.net/endless2/websites/" + str(website_id) + "/content_items/"
            url_store = "https://apps.squarespace.net/endless2/websites/" + str(website_id) + "/content_collections/"
        else:
            url_item = 'no more'
            url_store = 'no more'
        
        item_url, collection_id = get_item_url_id(session, url_item, item.get('product_id'), LIMIT)

        if item_url != 'none' and collection_id != 'none':
            store_url = get_store_url_id(session, url_store, collection_id, LIMIT)
        
        if item_url != 'none' and store_url != 'none':
            primary_domain = item.get('primary_domain', item.get('identifier', 'none')+'.squarespace.com')
            item['direct_url'] = primary_domain + '/' + store_url + '/' + item_url
    
        temp_dict = copy.deepcopy(item)
        dict_list.append(temp_dict)
        
    return data

def get_top10_products(start_date, end_date):
    d = get_top_products(start_date, end_date)
    d = add_item_urls(d)

    return d