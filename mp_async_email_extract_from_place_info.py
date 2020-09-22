import time
import numpy as np
import glob
import pandas as pd

def parse_emails(df):
    website=df['website'].values[0]    
    try:
        allLinks = [];mails=[]
        url = website
        response = requests.get(url)
        soup = BeautifulSoup(response.text, 'html.parser')
        links = [a.attrs.get('href') for a in soup.select('a[href]') ]
        for i in links:
            if(("contact" in i or "Contact")or("Career" in i or "career" in i))or('about' in i or "About" in i)or('Services' in i or 'services' in i):
                allLinks.append(i)
        allLinks=set(allLinks)
        def findMails(soup):
            for name in soup.find_all('a'):
                if(name is not None):
                    emailText=name.text
                    match=bool(re.match('[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$',emailText))
                    if('@' in emailText and match==True):
                        emailText=emailText.replace(" ",'').replace('\r','')
                        emailText=emailText.replace('\n','').replace('\t','')
                        if(len(mails)==0)or(emailText not in mails):
                            print(website,emailText)
                        mails.append(emailText)
        for link in allLinks:
            if(link.startswith("http") or link.startswith("www")):
                r=requests.get(link)
                data=r.text
                soup=BeautifulSoup(data,'html.parser')
                findMails(soup)

            else:
                newurl=url+link
                r=requests.get(newurl)
                data=r.text
                soup=BeautifulSoup(data,'html.parser')
                findMails(soup)

        mails=list(set(mails))
        if(len(mails)==0):
            print("NO MAILS FOUND")
    except:
        print("SSL")
        pass
    if len(mails)>0:
        df_email = pd.DataFrame({'email':mails})
        try:
            df_email.to_csv('result_school/'+str(df['city'].values[0])+'-'+str(df['name'].values[0])+'.csv',index=False)
        except:
            print("exception")
            df_email.to_csv('result_school/'+str(df['city'].values[0])+'-'+str(df['phone'].values[0])+'.csv',index=False)
    return None

start = time.time()
web_unq = len(set(df_schools.dropna()['website']))
split_dfs = np.array_split(df_schools.dropna(),web_unq, axis=0)
pool = mp.Pool(mp.cpu_count()-2) 
async_result = pool.map_async(parse_emails,split_dfs)

path = r'result_school/' # use your path
all_files = glob.glob(path + "/*.csv")

li = []
for filename in all_files:
    df = pd.read_csv(filename)
    li.append(df)

frame = pd.concat(li, axis=0, ignore_index=True)