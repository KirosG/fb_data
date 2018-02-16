import requests
import datetime
import csv

def csv_statuses():
    with open('%s_facebook_statuses.csv' % page_id, 'w') as file:
        writer = csv.DictWriter(file, fieldnames=fieldnames_statuses)
        writer.writeheader()

def csv_comments():
    with open('%s_facebook_comments.csv' % page_id, 'w') as file:
        writer = csv.DictWriter(file, fieldnames=fieldnames_comments)
        writer.writeheader()
    
# Holt JSON Daten von Facebook
def getposts():
    '''
    Holt erste Runde Statuses
    Start-Runde, danach wird statuses immer neu zugewiesen mit
    url = statuses['paging']['next']
    '''
    url = 'https://graph.facebook.com/v2.6/%s/posts' % page_id
    params = {
            'fields': 'id,message,permalink_url,link,name,created_time,comments.filter(stream).summary(true).limit(0)',
            'limit': '100',
            'since': since_unix,
            'until': until_unix,
            'access_token': access_token
                }
    
    r = requests.get(url, params=params)
    
    if r.status_code == 200:
        statuses = r.json()
        formatTime(statuses)
        return statuses
    else:
        print('No Data received on', r.url)
        return None

def getcomments(parent_id):
    '''
    Holt erste Runde comments, Start-Runde, danach wird comments
    immer neu zugewiesen mit url = comments['paging']['next']
    '''
    
    url = 'https://graph.facebook.com/v2.6/%s/comments' % parent_id
    params = {
            'fields': 'id,message,created_time,from,permalink_url,attachment,object,parent',
            'limit': '250',
            'summary': 'true',
            'filter': 'stream',
            'access_token': access_token
            }
    
    r = requests.get(url, params=params)
    
    if r.status_code == 200:
        comments = r.json()
        if bool(comments['data']):
            formatTime(comments)
            return comments
        else:
            return None
    else:
        print('No Data received on', r.url)
        return None

def formatTime(statuses):
    ''' Importiert die Zeiten in einem Datenset zu datetime'''
    
    for status in statuses['data']:
        timezonediff = datetime.timedelta(hours=1)
        # Ändert das Format der created_time des geloopten Status
        posttimeformat = datetime.datetime.strptime(status['created_time'], '%Y-%m-%dT%H:%M:%S+%f')
        currentposttime = posttimeformat + timezonediff
        status['created_time'] = currentposttime


def scrapeposts():
    ''' Scraped Posts und schreibt sie in CSV '''
    
    global status_ids
    
    print('\nScraping Posts from %s at: %s' % (page_id, datetime.datetime.now()))
    num_posts = 0
    
    statuses = getposts()

    hasnextpage = True
    while hasnextpage:
        # Öffnet Statuses CSV mit append
        with open('%s_facebook_statuses.csv' % page_id, 'a') as file:

            # Initialisiert DictWriter
            writer = csv.DictWriter(file, fieldnames=fieldnames_statuses, \
                                    extrasaction='ignore')          
            # Geht Zeile für Zeile durch die Daten auf Post-Ebene
            for status in statuses['data']:
                
                status['total_count'] = status['comments']['summary']['total_count']
                del status['comments']
                
                writer.writerow(status)
                status_ids.append(status['id'])
                
                num_posts += 1
                if num_posts % 10 == 0:
                    print(num_posts, 'Posts Scraped')
                if longform:
                    if 'message' in status:
                        print('Post:', status['message'][:70])
                    else:
                        print('No Message')
                    
    
            # Durchgang der Datenverarbeitung beendet
            # Nächste Seite
            if 'next' in statuses['paging']:
                if 'https:' in statuses['paging']['next']:
                    r = requests.get(statuses['paging']['next'])
                    if r.status_code == 200:
                        statuses = r.json()
                        formatTime(statuses)
                    else:
                        print('No Data received on', r.url)
                else:
                    print('Stopped scraping posts for paging')
                    hasnextpage = False
            else:
                print('Stopped scraping posts for paging')
                hasnextpage = False

    scrapecomments()

def scrapecomments():
    # Zeit des aktuellen Scrapes
    print('Scraping comments from %s at: %s' % (page_id, datetime.datetime.now()))
    # # # # # # # #
    # Kommentare  #
    # # # # # # # #
    num_comments = 0

    for status_id in status_ids:
        print('Scraping comments for post', status_id)
        num_comments_post = 0
        comments = getcomments(status_id)
        
        # Weiter, wenn es Kommentare zum Post gibt
        if comments is not None:
            hasnextpage = True
            while hasnextpage:
                with open('%s_facebook_comments.csv' % page_id, 'a') as file:

                    # Initialisiert DictWriter
                    writer = csv.DictWriter(file, fieldnames_comments, extrasaction='ignore')

                    for comment in comments['data']:
    
                        # Daten strukturieren für CSV
                        
                        # User-Infos
                        comment['user_id'] = comment['from']['id']
                        comment['user_name'] = comment['from']['name']
                        del comment['from']
                        
                        # Object-Infos
                        if 'object' in comment:
                            comment['post_id'] = status_id
                            if 'description' in comment['object']:
                                comment['post_message'] = comment['object']['description']
                            del comment['object']
                        
                        # Attachment-Infos
                        if 'attachment' in comment:
                            if 'title' in comment['attachment']:
                                comment['attachment_title'] = comment['attachment']['title']
                            comment['attachment_type'] = comment['attachment']['type']
                            comment['attachment_url'] = comment['attachment']['target']['url']
                            del comment['attachment']
                        
                        # Comment-Level
                        if 'parent' in comment:
                            comment['slc'] = 1
                            comment['parent_id'] = comment['parent']['id']
                            comment['parent_message'] = comment['parent']['message']
                            del comment['parent']
                        else:
                            comment['tlc'] = 1
                        
                        writer.writerow(comment)
                        num_comments += 1
                        num_comments_post +=1
                        if num_comments % 100 == 0:
                            print(num_comments, 'comments scraped')
                        if longform:
                            print('     ', comment['message'][:70]\
                                      .replace('\n', ' '))
                    
                    # Runde Datenverarbeitung beendet, nächste Seite
                    if 'next' in comments['paging']:
                        if 'https' in comments['paging']['next']:
                            r = requests.get(comments['paging']['next'])
                            if r.status_code == 200:
                                comments = r.json()
                                formatTime(comments)
                            else:
                                print('No Data received on', r.url)
                        else:
                            print(num_comments_post, 'comments on this post')
                            hasnextpage = False
                    else:
                        print(num_comments_post, 'comments on this post')
                        hasnextpage = False

    print('Done!', num_comments, 'Comments of', len(status_ids), \
          'Posts Processed in %s' % (datetime.datetime.now() - scrape_starttime))


if __name__ == '__main__':

    access_token = input('Input Access Token: ')
    page_id = input('Input Page: ')
    status_ids = []

    scrape_starttime = datetime.datetime.now()
    
    since_unix = '1502575200'    
    until_unix = '1506290399'
        
    longform = input('Display all data? y/n: ')
    if 'y' in longform:
        longform = True
    else:
        longform = False
    
    fieldnames_statuses = ['id', 'message', 'name', 'total_count', 'link', 'created_time', 'permalink_url']
    fieldnames_comments = ['id', 'message', 'attachment_title', 'attachment_type', \
                      'attachment_url', 'user_id', 'user_name', 'created_time', \
                      'post_id', 'post_message', 'tlc', 'slc', 'parent_id', \
                      'parent_message', 'permalink_url']
    csv_statuses()
    csv_comments()
    scrapeposts()