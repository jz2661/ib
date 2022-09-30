import logging,os
import pandas as pd
import numpy as np
from datetime import datetime
from email.mime.text import MIMEText

# Import smtplib for the actual sending function
import smtplib
import mimetypes

# Here are the email package modules we'll need
from email.message import EmailMessage

__all__ = ['expand_data','black','rank','remove_seen','send_mail']

def expand_data(data):
    return (data.date, data.title, data.company, data.apply_link, data.link, len(data.description), data.place)

def black(df):
    bl = [x.upper() for x in ['C++','Java','Sale','contract','summer','compliance','graduate','middle', \
            'intern','junior','control','RELATION','legal','student','human','operations','marketing', \
            'governance','account',]]
    mask = df['title'].apply(lambda x: any(kw in x.upper() for kw in bl))

    bl = [x.upper() for x in ['Argyll Scott','HSBC','DBS','Manulife','Selby','EY','HKIP','Hang Seng','AXA', \
            'McKinley','AIA','deloitte','Societe','prudential','kpmg','junan','consulting','agency','acca', \
            'Standard Chartered','agoda','wells','Recruitment',]]
    mask |= df['company'].apply(lambda x: any(kw in x.upper() for kw in bl))

    try:
        mask |= df['place'].apply(lambda x: all(lc not in x.upper() for lc in ['HONG KONG','SINGAPORE']))
    except:
        pass

    return df.drop(df.index[mask])

def rank(df):
    return df.sort_values('des',ascending=False).drop_duplicates(subset=['title'])

def remove_seen(df):
    try:
        cachedf = pd.read_excel('res.xlsx',index_col=0)
        cachedf['ap'] = cachedf['ap'].replace(np.nan, '')
        
        comb = pd.concat([df,cachedf]).drop_duplicates()
        comb.to_excel(f'res_{datetime.now().isoformat()[:-13]}.xlsx')
        comb.to_excel('res.xlsx')

        mask = df['title'].apply(lambda x: x not in cachedf['title'].values)
        return df[mask]
    except:
        raise Exception("Remove seen failed.")
        return df

def send_mail(files=[], df=None,):
    # Create the container email message.
    msg = EmailMessage()
    msg['Subject'] = 'ib_forecast'
    # me == the sender's email address
    # family = the list of all recipients' email addresses
    msg['From'] = 'zjzzjz2010@gmail.com'
    msg['To'] = 'zjzzjz2010@gmail.com'
    msg.preamble = 'You will not see this in a MIME-aware mail reader.\n'
    
    if not df is None:
        html = """\
        <html>
          <head></head>
          <body>
            {0}
          </body>
        </html>
        """.format(df.round(3).to_html())
        
        msg.set_content(html, subtype='html')

    # Open the files in binary mode.  Use imghdr to figure out the
    # MIME subtype for each specific image.
    for file in files:
        with open(file, 'rb') as fp:
            data = fp.read()
        ctype, encoding = mimetypes.guess_type(file)
        maintype, subtype = ctype.split('/', 1)
        msg.add_attachment(data,
                               maintype=maintype,
                               subtype=subtype,
                               filename=file)        

    # Send the email via our own SMTP server.
    with smtplib.SMTP(host="smtp.gmail.com", port="587") as smtp:  # 設定SMTP伺服器
        smtp.ehlo()  # 驗證SMTP伺服器
        smtp.starttls()  # 建立加密傳輸
        smtp.login("zjzzjz2010@gmail.com", "yrdxebidsnczndvq")  # 登入寄件者gmail
        smtp.send_message(msg)  # 寄送郵件
        print("Complete!")
