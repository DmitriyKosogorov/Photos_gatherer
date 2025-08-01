import requests
import undetected_chromedriver as uc
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver import ActionChains
import time
import pandas as pd
import random
import os

def cloud_flare_capcha(driver):
    #stop scraping when captcha occurs. wait until user pass it
    ps=driver.find_elements(By.XPATH,"//h1[@class='zone-name-title h1']")
    if(len(ps)>0):
        print('CloudFlare thing')
        while(True):
            time.sleep(2)
            ps=driver.find_elements(By.XPATH,"//h1[@class='zone-name-title h1']")
            print(len(ps))
            if(len(ps)==0):
                break
    print('cloudflare ended')
    
    
def parse_addresses():
    #scrape all addresses from KLADR
    collected_streets=[]
    if(os.path.isfile('streets.csv')):
        main_df=pd.read_csv('streets.csv')
        collected_streets=set(main_df['street'].tolist())
    else:
        main_df=pd.DataFrame(columns=['street','house_intervals','index','okato','tax'])
    print(collected_streets)
    
    URL='https://kladr-rf.ru/38/000/003/'
    driver = uc.Chrome(version_main=133)
    driver.get(URL)
    wrong_classes=['old', 'del']
    
    addresses=[]
    streets=[]
    indices=[]
    house_intervals=[]
    okato=[]
    nalogovaya=[]
    
    time.sleep(10)
    cloud_flare_capcha(driver)
    
    rows=driver.find_elements(By.CLASS_NAME, 'row')
    print(len(rows))
    lis=rows[4].find_elements(By.TAG_NAME, 'li')
    cur_len=len(lis)
    print(cur_len)
    for i in range(cur_len):
        if(i%100==0):
            print(f'{i}/{cur_len}')
        rows=driver.find_elements(By.CLASS_NAME, 'row')
        lis=rows[4].find_elements(By.TAG_NAME, 'li')
        street=lis[i].get_attribute('textContent')
        if(lis[i].get_attribute('class') not in wrong_classes and street not in collected_streets):
            print(street)
            counter=0

            button_found=False
            command="arguments[0].scrollTop = arguments[0].scrollHeight"
            while(True):
                if(counter>100 or button_found==True):
                    break
                try:
                    lis[i].find_element(By.TAG_NAME,'a').click() #try click on link. If unclickable, you need to scroll to it
                    button_found=True
                except:
                    counter+=1
                    scrollable_div=driver.find_element(By.TAG_NAME,'body')
                    driver.execute_script(command, scrollable_div)
                    ActionChains(driver).scroll_to_element(lis[i]).perform()
                    time.sleep(1+round(random.uniform(0, 1), 2))
            
            if(button_found==False):
                continue
            time.sleep(2+round(random.uniform(0, 3), 2))
            cloud_flare_capcha()
            
            tables=driver.find_elements(By.XPATH,"//table[@class='table table-bordered table-hover']")
            if(len(tables)<2):
                main_df.loc[len(main_df)]=[street, 'empty', 'empty', 'empty', 'empty']
            else:
                cur_table=tables[1]
                tds=cur_table.find_elements(By.TAG_NAME, 'td')
                for i in range(0, len(tds), 4):
                    house_interval=tds[i].get_attribute('textContent')
                    index=tds[i+1].get_attribute('textContent')
                    okato=tds[i+2].get_attribute('textContent')
                    nalogovaya=tds[i+3].get_attribute('textContent')
                    print([street, house_interval, index, okato, nalogovaya])
                    main_df.loc[len(main_df)]=[street, house_interval, index, okato, nalogovaya]
            driver.execute_script("window.history.go(-1)")
            time.sleep(3+round(random.uniform(0, 3), 2))
            cloud_flare_capcha()
            main_df.to_csv('streets.csv', index=False)
        
        
    driver.close()
    result=pd.DataFrame()
    result['street']=streets
    result['house_intervals']=house_intervals
    result['index']=indices
    result['okato']=okato
    result['tax']=nalogovaya
    result.to_csv('addresses_raw.csv')

    
    
def cut_file():
    #cut house interval cells in tanble so it 
    streets_df=pd.read_csv('streets.csv')
    print(streets_df.columns)
    streets_df['house_intervals']=streets_df['house_intervals'].str.split(',')
    streets_df=streets_df.explode('house_intervals')
    streets_df.to_csv('streets_exploded.csv')


if __name__=='__main__':
    parse_addresses()
    cut_file()
