from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.action_chains import ActionChains
from webdriver_manager.chrome import ChromeDriverManager
from selenium import webdriver
from datetime import datetime
from threading import Thread
from natasha import AddrExtractor, MorphVocab
import requests
import shutil
import nltk
import json
import time
import random
import pandas as pd
import re
import os


def json_read(filename: str):
    with open(filename) as f_in:
        return json.load(f_in)
    
    
def parse_id(link):
    found=None
    found=re.findall(r'\/\d+\?',link)
    if(len(found)==0):
        found=re.findall(r'\/\d+$',link)
        if(len(found)==0):
            found=None
        else:
            found=found[0]
    else:
        found=found[0].replace('?','')
    if(found!=None):
        found=found.replace('/','')
    return found
    
    
class Parser():
    
    def __init__(self):
        #self.already_taken=pd.read_csv('gathered_data_2/2gis/organizations/total.csv')['2gis_id'].astype(str).tolist()
        #self.already_taken_requests=pd.read_csv('gathered_data_2/2gis/organizations/total.csv')['original_address'].astype(str).tolist()
        #self.already_taken=pd.read_csv('Иркутск, улица Лермонтова, _20-28__2gis_org_links21.csv')['2gis_id'].astype(str).tolist()
        self.already_taken=[]
        self.save_organization_pd=pd.DataFrame(columns=['original_address','found_place','found_address','coords','place_name','place_link','2gis_id','inside_original', 'original_for_inside'])
        self.photos_organization_pd=pd.DataFrame(columns=['original_address','found_place','coords','place_name','place_link','image_url','outside', '2gis_id'])
        self.photos_building_pd=pd.DataFrame(columns=['original_address','found_place','found_address','coords', 'place_link', 'photo_link', '2gis_id', 'is_panorama'])
        self.photos_additional=pd.DataFrame(columns=['original_address','found_place','coords','place_name','place_link','image_url','inside','2gis_id','supposed_number_of_photos'])
        self.places_parsed=pd.DataFrame(columns=['original_address','found_place','found_address','coords','address_link','2gis_id'])
        cService = webdriver.ChromeService(executable_path='./chromedriver_136')
        self.driver=webdriver.Chrome(service = cService)
        self.divs_dict=json_read('tags.json')
        self.base_URL='https://2gis.ru/irkutsk'
        self.clicked_hrefs=[]
        self.streets_dict={'1-й Берег Ангары Улица':'улица Берег Ангары, 1-й переулок', '2-й Берег Ангары Улица':'улица Берег Ангары, 2-й переулок', "1-й Ленинградский Переулок":"Ленинградский переулок",
                           '1-ый Ленинский квартал Территория':'Первый Ленинский Квартал', '2-й Район Территория':''}
        self.wrong_places=['Иркутск','Падь Мельничная','река Ангара']
        self.wrong_types=['СНТ','СПК', 'Некоммерческое СТ', 'некоммерческое СТ', 'Садоводческое товарищество', 'Садоводческий кооператив',
                     'садоводческого кооператива','Садоводческое товарищество','ДНТ']
        self.status_codes={'wrong_type':-2,'nothing_found':-1,'not_collected': 0, 'collected':1, 'error_in_code':-3}
        
        
    
    def recreate_driver(self):
        cService = webdriver.ChromeService(executable_path='./chromedriver_136')
        self.driver=webdriver.Chrome(service = cService)
        
        
 
    
    #saves file with organizations link==============================================    
    def save_org_pd(self, base_name=None):
        if(base_name==None):
            base_name='2gis_org_links'
        else:
            base_name=base_name+'_2gis_org_links'
        number=0
        filenames=os.listdir(os.path.abspath(os.curdir))
        for filename in filenames:
            if(base_name in filename):
                numbers=re.findall(r'\d+', filename)
                if(len(numbers)>0 and int(numbers[-1])>number):
                    number=int(numbers[-1])
        number+=1
        self.save_organization_pd.to_csv(base_name+str(number)+'.csv')
    
        
    
    #saves file with photos link================================================
    def save_photo_pd(self, base_name=None):
        if(base_name==None):
            base_name='2gis_photos_links'
        else:
            base_name=base_name+'_2gis_photos_links'
        number=0
        filenames=os.listdir(os.path.abspath(os.curdir))
        for filename in filenames:
            if(base_name in filename):
                numbers=re.findall(r'\d+', filename)
                if(len(numbers)>0 and int(numbers[-1])>number):
                    number=int(numbers[-1])
        number+=1
        self.photos_organization_pd.to_csv(base_name+str(number)+'.csv')
        
    
    #saves file with photos of building itself====================================
    def save_building_photos_pd(self, base_name=None):
        if(base_name==None):
            base_name='2gis_photos_building_links'
        else:
            base_name=base_name+'_2gis_photos_building_links'
        number=0
        filenames=os.listdir(os.path.abspath(os.curdir))
        for filename in filenames:
            if(base_name in filename):
                numbers=re.findall(r'\d+', filename)
                if(len(numbers)>0 and int(numbers[-1])>number):
                    number=int(numbers[-1])
        number+=1
        self.photos_building_pd.to_csv(base_name+str(number)+'.csv')
        
    
    def save_additional_photos_pd(self, base_name=None):
        if(base_name==None):
            base_name='2gis_photos_additional_links'
        else:
            base_name=base_name+'_2gis_photos_additional_links'
        number=0
        filenames=os.listdir(os.path.abspath(os.curdir))
        for filename in filenames:
            if(base_name in filename):
                numbers=re.findall(r'\d+', filename)
                if(len(numbers)>0 and int(numbers[-1])>number):
                    number=int(numbers[-1])
        number+=1
        self.photos_additional.to_csv(base_name+str(number)+'.csv')
    
        
    
    #checks if the page was moved to 2gis uncapha===============================
    def check_limited(self):
        limiteds=self.driver.find_elements(By.TAG_NAME,'pre')
        if(len(limiteds)>0):
            for limited in limiteds:
                if(limited.get_attribute('textContent')=='limited'):
                    return True
        return False
        
    
    
    # scrolling down until no updates on page
    def scroll_down(self, scrollable_div, list_element_name, list_element_key, min_elem_count=0, counter=3):
        new_len=len(self.driver.find_elements(list_element_key, list_element_name))
        old_len=0
        def_count=0
        less_than_current=0
        rescroll_count=0
        while(def_count<counter):
            #print(new_len)
            command="arguments[0].scrollTop = arguments[0].scrollHeight"
            self.driver.execute_script(command, scrollable_div)
            time.sleep(round(random.uniform(0.5, 1), 2))
            new_len=len(self.driver.find_elements(list_element_key, list_element_name))
            #if(new_len>=min_elem_count):
            #    break
            if(new_len==old_len):
                def_count+=1
            else:
                old_len=new_len
            if(def_count>=counter and new_len<min_elem_count):
                def_count=0
                print(f'rescroll: {list_element_name}, {min_elem_count}')
                rescroll_count+=1
                for i in range(100):
                    command=f"arguments[0].scrollTop = arguments[0].scrollHeight*{i}/{100}"
                    self.driver.execute_script(command, scrollable_div)
                    time.sleep(0.1)
                if(rescroll_count>10):
                    return False
                    print(f'Trash: {list_element_name}, {min_elem_count}')
        return True
                    

        
    #get all links on photos of organization=======================================
    def parse_photos(self, URL, found_place, coords, org_name, address, dgis_id, index):
        mid_photos_organization_pd=pd.DataFrame(columns=['original_address','found_place','coords','place_name','place_link','image_url','outside','2gis_id'])
        self.driver.get(URL)
        time.sleep(3+round(random.uniform(0, 1), 2))
        photos_divs=[]
        carusel_tags=self.driver.find_elements(By.CLASS_NAME, self.divs_dict['2gis']['class_name']['carusel_element'])
        while(len(carusel_tags)==0):
            time.sleep(0.1)
            carusel_tags=self.driver.find_elements(By.CLASS_NAME, self.divs_dict['2gis']['class_name']['carusel_element'])
        isinside=False
        clicked_photos=0
        for carusel_tag in carusel_tags:
            if(clicked_photos!=0):
                break
            link=carusel_tag.find_element(By.TAG_NAME,'a')
            #print(re.sub(r'\d', '', link.get_attribute('textContent')))
            if(re.sub(r'\d', '', link.get_attribute('textContent'))=='Фото'):
                clicked_photos=0
                number_photos=re.findall(r'\d+', link.get_attribute('textContent'))
                if(len(number_photos)>0):
                    number_photos=int(number_photos[0])
                else:
                    number_photos=0
                    return -1
                while(clicked_photos==0):
                    try:
                        link.click()
                        time.sleep(3)
                        clicked_photos=1
                    except:
                        scrolls_right=self.driver.find_elements(By.CLASS_NAME, self.divs_dict['2gis']['class_name']['scroll_right'])
                        if(len(scrolls_right)>0):
                            scrolls_right[0].click()
                            time.sleep(1)
                        else:
                            clicked_photos=2
                no_photos=self.driver.find_elements(By.CLASS_NAME, self.divs_dict['2gis']['class_name']['add_photos'])
                if(len(no_photos)>0):
                    continue
                more_tags=self.driver.find_element(By.CLASS_NAME, self.divs_dict['2gis']['class_name']['additional_panel']).find_elements(By.CLASS_NAME,self.divs_dict['2gis']['class_name']['more_photos_types'])
                if(len(more_tags)>0):
                    more_tags[-1].click()
                    time.sleep(1)
                outside_tags=self.driver.find_elements(By.CLASS_NAME,self.divs_dict['2gis']['class_name']['photos_type'])
                num_outside_pht=0
                for outside_tag in outside_tags:
                    tag_text=re.sub(r'\d', '', outside_tag.get_attribute('textContent'))
                    if(tag_text=='Снаружи'):
                        num_outside_pht=int(re.findall(r'\d+', outside_tag.get_attribute('textContent'))[0])
                        outside_tag.click()
                        isinside=True
                        time.sleep(1)
                if(num_outside_pht==0):
                    min_count=number_photos
                else:
                    min_count=num_outside_pht
                scrollable_divs=self.driver.find_elements(By.CLASS_NAME, self.divs_dict['2gis']['class_name']['scrollable_div'])
                if(len(scrollable_divs)>0):
                    scrollable_div=scrollable_divs[0]
                    scroll_result=self.scroll_down(scrollable_div, self.divs_dict['2gis']['class_name']['photos_divs'], By.CLASS_NAME, min_count)
                if(scroll_result==False):
                    return
                photo_list=self.driver.find_elements(By.CLASS_NAME,self.divs_dict['2gis']['class_name']['photos_list'])
                photo_divs=photo_list[0].find_elements(By.CLASS_NAME,self.divs_dict['2gis']['class_name']['photos_divs'])
                for photo_div in photo_divs:
                    photo_link=photo_div.get_attribute('style')
                    mid_photos_organization_pd.loc[len(mid_photos_organization_pd)]=[address, found_place, coords, org_name, URL, photo_link, isinside, dgis_id]
                                                    #['original_address','found_place','coords','place_name','place_link','image_url','outside']
        self.photos_organization_pd=pd.concat([self.photos_organization_pd, mid_photos_organization_pd], ignore_index=True)
        return 1
                        
        
    
    def recheck_photos(self):
        org_pd=pd.read_csv('gathered_data_1/2gis/organization_links/total.csv')
        org_pht_pd=pd.read_csv('gathered_data_1/2gis/organization_photos/total.csv')
        build_pht_pd=pd.read_csv('gathered_data_1/2gis/organization_photos/total.csv')
        #org_pd=org_pd.loc[org_pd['2gis_id']==1548748027219919]
        print(len(org_pd))
        total_pht_pd=pd.read_csv('gathered_data_1/2gis/total_photos/total.csv')
        
        mid_photos_additional_pd=pd.DataFrame(columns=['original_address','found_place','coords','place_name','place_link','image_url','inside','2gis_id','supposed_number_of_photos'])
        photos_divs=[]
        log_file=open('log_file.txt', 'a')        
        k=-1
        for i,row in org_pd.iterrows():
            k+=1
            if(k<152):
                continue
            print(f'{k}/{len(org_pd)}')
            if(k%5==0 and k>0):
                self.photos_additional=pd.concat([self.photos_additional, mid_photos_additional_pd])
                self.photos_additional.drop_duplicates(subset=['image_url'])
                self.save_all('Академгородок_')
            #print(row['2gis_id'])
            total_len=len(total_pht_pd.loc[total_pht_pd['2gis_id']==row['2gis_id']])
            self.driver.get(row['place_link'])
            #self.driver.get('https://2gis.ru/irkutsk/firm/1548640652907009')
            time.sleep(5+round(random.uniform(0, 2), 2))
            
            carusel_tags=self.driver.find_elements(By.CLASS_NAME, self.divs_dict['2gis']['class_name']['carusel_element'])
            isinside=False
            clicked_photos=0
            prev_photos_num=len(self.driver.find_elements(By.CLASS_NAME,self.divs_dict['2gis']['class_name']['photos_divs']))
            
            for carusel_tag in carusel_tags:
                #print(carusel_tag.get_attribute('textContent'))
                if(clicked_photos!=0):
                    break
                link=carusel_tag.find_element(By.TAG_NAME,'a')
                #print(re.sub(r'\d', '', link.get_attribute('textContent')))
                if(re.sub(r'\d', '', link.get_attribute('textContent'))=='Фото'):
                    clicked_photos=0
                    number_photos=re.findall(r'\d+', link.get_attribute('textContent'))
                    if(len(number_photos)>0):
                        number_photos=int(number_photos[0])
                    else:
                        number_photos=0
                    
                    print(f'{row["2gis_id"]}: {total_len}/{number_photos}')
                    log_file.write(f'{row["2gis_id"]}: {total_len}/{number_photos}\n')
                    if(number_photos>total_len):
                        while(clicked_photos==0):
                            try:
                                link.click()
                                time.sleep(3)
                                clicked_photos=1
                            except:
                                scrolls_right=self.driver.find_elements(By.CLASS_NAME, self.divs_dict['2gis']['class_name']['scroll_right'])
                                if(len(scrolls_right)>0):
                                    scrolls_right[0].click()
                                    time.sleep(1)
                                else:
                                    clicked_photos=2
                        no_photos=self.driver.find_elements(By.CLASS_NAME, self.divs_dict['2gis']['class_name']['add_photos'])
                        if(len(no_photos)>0):
                            continue
                        more_tags=self.driver.find_element(By.CLASS_NAME, self.divs_dict['2gis']['class_name']['additional_panel']).find_elements(By.CLASS_NAME,self.divs_dict['2gis']['class_name']['more_photos_types'])
                        if(len(more_tags)>0):
                            more_tags[-1].click()
                            time.sleep(2)
                        outside_tags=self.driver.find_elements(By.CLASS_NAME,self.divs_dict['2gis']['class_name']['photos_type'])
                        num_outside_pht=0
                        for outside_tag in outside_tags:
                            tag_text=re.sub(r'\d', '', outside_tag.get_attribute('textContent'))
                            if(tag_text=='Снаружи'):
                                num_outside_pht=int(re.findall(r'\d+', outside_tag.get_attribute('textContent'))[0])
                                outside_tag.click()
                                isinside=True
                                time.sleep(1)
                        if(num_outside_pht==0):
                            min_count=number_photos
                        else:
                            min_count=num_outside_pht
                        print(f'{row["2gis_id"]}: {total_len}/{min_count}')    
                        log_file.write(f'{row["2gis_id"]}: {total_len}/{min_count}\n')
                        if(min_count>total_len):
                            scrollable_divs=self.driver.find_elements(By.CLASS_NAME, self.divs_dict['2gis']['class_name']['scrollable_div'])
                            if(len(scrollable_divs)>0):
                                scrollable_div=scrollable_divs[0]
                                scroll_result=self.scroll_down(scrollable_div, self.divs_dict['2gis']['class_name']['photos_divs'], By.CLASS_NAME, min_count)
                            if(scroll_result==False):
                                return
                            photo_list=self.driver.find_elements(By.CLASS_NAME,self.divs_dict['2gis']['class_name']['photos_list'])
                            if(len(photo_list)>0):
                                photo_divs=photo_list[0].find_elements(By.CLASS_NAME,self.divs_dict['2gis']['class_name']['photos_divs'])
                            else:
                                photo_divs=self.driver.find_elements(By.CLASS_NAME,self.divs_dict['2gis']['class_name']['photos_divs'])
                            for photo_div in photo_divs:
                                photo_link=photo_div.get_attribute('style')
                                #print(photo_link)
                                mid_photos_additional_pd.loc[len(mid_photos_additional_pd)]=[row['original_address'], row['found_place'], row['coords'], row['place_name'], row['place_link'], photo_link, isinside, row['2gis_id'], min_count]
                                #                                                           ['original_address','found_place','coords','place_name','place_link','image_url','outside','2gis_id','supposed_number_of_photos']                           
                            print(f'new_image_len {row["2gis_id"]}: {len(photo_divs)}/{min_count}')
                            log_file.write(f'new_image_len {row["2gis_id"]}: {len(photo_divs)}/{min_count}\n')
                time.sleep(2+round(random.uniform(0, 2), 2))
        self.photos_additional=pd.concat([self.photos_additional, mid_photos_additional_pd])
        log_file.close()
         
            
            
            
        
    #parses places organizations inside builing with given address=================
    def search_places_by_address(self, URL, address, download_photos=True, index=-1):
        for wrong_type in self.wrong_types:
            if(wrong_type in address):
                return -2
        #print(f'save_org_pd: {self.save_organization_pd["found_place"].tolist()}')
        input_tag=self.driver.find_elements(By.TAG_NAME, self.divs_dict['2gis']['tag_name']['input_line'])
        if(len(input_tag)==0):
            self.driver.get(URL)
            time.sleep(3+round(random.uniform(0, 3), 2))
        time.sleep(0.5)
        mid_organizations_pd=pd.DataFrame(columns=['original_address','found_place','found_address','coords','place_name','place_link','2gis_id','inside_original', 'original_for_inside'])
        mid_photos_building_pd=pd.DataFrame(columns=['original_address','found_place','found_address','coords', 'place_link', 'photo_link', '2gis_id', 'is_panorama'])


        
        input_tag=self.driver.find_elements(By.TAG_NAME, self.divs_dict['2gis']['tag_name']['input_line'])
        if(len(input_tag)>0):
            input_tag=input_tag[0]
            time.sleep(0.5)
            input_tag.click()
            time.sleep(0.5)
            input_tag.send_keys(Keys.CONTROL + "a")
            time.sleep(0.5)
            input_tag.send_keys(Keys.DELETE)
            time.sleep(0.5)
            input_tag.send_keys(address)
            time.sleep(1+round(random.uniform(0, 3), 2))
            
            input_tag.send_keys(Keys.ENTER)
            
            search_results=self.driver.find_elements(By.CLASS_NAME, self.divs_dict['2gis']['class_name']['found_addresses_unpressed'])
            search_results1=self.driver.find_elements(By.CLASS_NAME, self.divs_dict['2gis']['class_name']['found_addresses_pressed'])
            search_resultsh=self.driver.find_elements(By.CLASS_NAME, self.divs_dict['2gis']['class_name']['found_addresses_hover'])
            len_res=max(len(search_results), len(search_results1), len(search_resultsh))
            check_loading=self.driver.find_elements(By.CLASS_NAME, self.divs_dict['2gis']['class_name']['loading_search_results'])
            nothing_found=self.driver.find_elements(By.CLASS_NAME, self.divs_dict['2gis']['class_name']['nothing_found'])
                    
            #print(f'res:{len(search_results)}, noth:{len(nothing_found)}, load:{len(check_loading)}')
            
            #for search_result in search_results:
            #    print(search_result.get_attribute('innerHTML'))
            
            start_timer=time.time()
            while(len(nothing_found)==0 and (len_res==0 or (len_res!=0 and len(check_loading)!=0))):
                #print(f'res:{len(search_results)}, noth:{len(nothing_found)}, load:{len(check_loading)}')
                time.sleep(0.1)
                search_results=self.driver.find_elements(By.CLASS_NAME, self.divs_dict['2gis']['class_name']['found_addresses_unpressed'])
                search_results1=self.driver.find_elements(By.CLASS_NAME, self.divs_dict['2gis']['class_name']['found_addresses_pressed'])
                search_resultsh=self.driver.find_elements(By.CLASS_NAME, self.divs_dict['2gis']['class_name']['found_addresses_hover'])
                len_res=max(len(search_results), len(search_results1), len(search_resultsh))
                check_loading=self.driver.find_elements(By.CLASS_NAME, self.divs_dict['2gis']['class_name']['loading_search_results'])
                nothing_found=self.driver.find_elements(By.CLASS_NAME, self.divs_dict['2gis']['class_name']['nothing_found'])
                if(len(search_results)>0 and time.time()-start_timer>5 and len(check_loading)>0):
                    ActionChains(self.driver).move_to_element(check_loading[0]).perform()
                    time.sleep(0.5)
            
            
            if(len(nothing_found)>0):
                return -1 
            
            
                #print(f'pages_scrolled: {pages_scrolled}')
            search_resultsh=self.driver.find_elements(By.CLASS_NAME, self.divs_dict['2gis']['class_name']['found_addresses_hover'])
            input_tag=self.driver.find_elements(By.TAG_NAME, self.divs_dict['2gis']['tag_name']['input_line'])
            if(len(search_resultsh)>0 and len(input_tag)>0):
                ActionChains(self.driver).move_to_element(input_tag[0]).perform()
            
            search_results=self.driver.find_elements(By.CLASS_NAME, self.divs_dict['2gis']['class_name']['found_addresses_unpressed'])
            scrollable_divs=self.driver.find_element(By.CLASS_NAME, self.divs_dict['2gis']['class_name']['main_panel']).find_elements(By.CLASS_NAME, self.divs_dict['2gis']['class_name']['scrollable_div'])
            #print(f'len scroll: {len(scrollable_divs)}')
            
            if(len(scrollable_divs)>0):
                scrollable_div=scrollable_divs[0]
            
           
            #print(f'len(search_result): {len(search_results)}')
            if(len(search_results)==0):
                search_results=self.driver.find_elements(By.CLASS_NAME, self.divs_dict['2gis']['class_name']['found_addresses_pressed'])
                if(len(search_results)==0):
                    return 0
                
                    
            prev_name='%'
            for i, search_result in enumerate(search_results):
                if(i>0):
                    return 1
                #print(f'places_number: {places_number}, scrolled_perc:{scroll_perc}')
                ActionChains(self.driver).scroll_to_element(search_result).perform()
                tagg=search_result.find_elements(By.TAG_NAME, 'a')
                if(len(tagg)>0):
                    hreff=tagg[0].get_attribute('href')
                    found_name=tagg[0].get_attribute('textContent')
                else:
                    hreff=''
                #print('$ ',hreff,' $')
                hreff_code=re.findall(r'\/\d+\?', hreff)
                if(len(hreff_code)>0):
                    hreff_code=hreff_code[0].replace('/','').replace('?','')
                else:
                    hreff_code='-'
                #print(hreff_code)
                print(f'found_name: %{found_name}%')
                if(found_name in self.wrong_places):
                    return -2
                else:
                    self.clicked_hrefs.append(hreff_code)
        
                clicked=False
                while(clicked==False):
                    try:
                        tagg[0].click()
                        clicked=True
                        time.sleep(2)
                    except:
                        command="arguments[0].scrollTop = arguments[0].scrollHeight*0.1"
                        self.driver.execute_script(command, scrollable_div)
                
                #print('clicked')
                house_tags=self.driver.find_elements(By.CLASS_NAME, self.divs_dict['2gis']['class_name']['place_name'])
                while(len(house_tags)==0):
                    house_tags=self.driver.find_elements(By.CLASS_NAME, self.divs_dict['2gis']['class_name']['place_name'])
                    house_tags_closed=self.driver.find_elements(By.CLASS_NAME, self.divs_dict['2gis']['class_name']['place_name_closed'])
                    if(len(house_tags)==0 and len(house_tags_closed)>0):
                        house_tags=house_tags_closed
                    if(len(house_tags)>0):
                        house_name=house_tags[0].get_attribute('textContent')
                        if(house_name==prev_name):
                            house_tags=[]
                        
                    time.sleep(0.1)
                
                house_tags=self.driver.find_elements(By.CLASS_NAME, self.divs_dict['2gis']['class_name']['place_name'])
                house_tags_closed=self.driver.find_elements(By.CLASS_NAME, self.divs_dict['2gis']['class_name']['place_name_closed'])
                closed_place=False
                if(len(house_tags)==0 and len(house_tags_closed)>0):
                    house_tags=house_tags_closed
                    closed_place=True
                if(len(house_tags)>0):
                    house_name=house_tags[0].get_attribute('textContent')
                else:
                    house_name='Error'
                print(house_name)
                prev_name=house_name
                coords_tag=self.driver.find_elements(By.CLASS_NAME, self.divs_dict['2gis']['class_name']['coords_div'])
                coords='-'
                if(len(coords_tag)>0):
                    coords=coords_tag[0].get_attribute('textContent')
                else:
                    coords=re.findall(r'\/%2C\d+\?',self.driver.current_url)
                    if(len(coords)>0):
                        coords=coords[0]
                    else:
                        coords='-'
                if(coords=='-'):
                    trace_tags=self.driver.find_elements(By.CLASS_NAME, self.divs_dict['2gis']['class_name']['trace'])
                    if(len(trace_tags)>0):
                        for trace_tag in trace_tags:
                            if(trace_tag.get_attribute('textContent')=='Проехать'):
                                coord_link=trace_tag.get_attribute('href')
                                coords=re.findall(r'\d+\.\d+%2C\d+\.\d+', coord_link)[0].replace('%2C',',')
                                #print(coords)
                
                if(closed_place==False):
                    potential_addresses=search_result.find_elements(By.CLASS_NAME, self.divs_dict['2gis']['class_name']['address_tag'])
                    if(len(potential_addresses)>0):
                        found_address=potential_addresses[0].get_attribute('textContent')
                    else:
                        found_address='error'
                else:
                    potential_addresses=search_result.find_elements(By.CLASS_NAME, self.divs_dict['2gis']['class_name']['address_tag_closed'])
                    if(len(potential_addresses)>0):
                        found_address=potential_addresses[0].get_attribute('textContent')
                    else:
                        found_address='error'
                #print(f'found_address: {found_address}')
                                
                mid_organizations_pd.loc[len(mid_organizations_pd)]=[address, house_name,found_address, coords, found_name, hreff, hreff_code, 0, hreff_code]   
                #                                           ['original_address','found_place','found_address','coords','place_name','place_link','2gis_id','inside_original']                         
                
                carusel_tags=self.driver.find_elements(By.CLASS_NAME, self.divs_dict['2gis']['class_name']['carusel_element'])
                #print(len(carusel_tags))
                
                for carusel_tag in carusel_tags:
                    link=carusel_tag.find_element(By.TAG_NAME,'a')
                    #print(re.sub(r'\d', '', link.get_attribute('textContent')))
                    if(re.sub(r'\d', '', link.get_attribute('textContent'))=='В здании'):
                        inside_orgs_re=re.findall(r'\d+', link.get_attribute('textContent'))
                        if(len(inside_orgs_re)>0):
                            inside_orgs_num=int(inside_orgs_re[0])
                        else:
                            inside_orgs_num=0
                        #print(f'inside orgs num: {len(inside_orgs_num)}')
                        if(inside_orgs_num>0):
                            clicked=0
                            while(clicked==0):
                                try:
                                    link.click()
                                    time.sleep(0.1)
                                    clicked=1
                                except:
                                    scroll_right=self.driver.find_elements(By.CLASS_NAME, self.divs_dict['2gis']['class_name']['scroll_right'])
                                    if(len(scroll_right)>0):
                                        scroll_right[0].click()
                                        time.sleep(2)
                                    else:
                                        clicked=1
                            loading_elems=self.driver.find_elements(By.CLASS_NAME, self.divs_dict['2gis']['class_name']['loading_search_results'])
                            while(len(loading_elems)>0):
                                loading_elems=self.driver.find_elements(By.CLASS_NAME, self.divs_dict['2gis']['class_name']['loading_search_results'])
                                time.sleep(0.1)    
                            scrollable_div1=self.driver.find_element(By.CLASS_NAME, self.divs_dict['2gis']['class_name']['additional_panel']).find_element(By.CLASS_NAME, self.divs_dict['2gis']['class_name']['scrollable_div'])
                            input_tag=self.driver.find_elements(By.TAG_NAME, self.divs_dict['2gis']['tag_name']['input_line'])
                            scroll_result=self.scroll_down(scrollable_div1, self.divs_dict['2gis']['class_name']['found_addresses_unpressed'], By.CLASS_NAME,inside_orgs_num-1)
                            if(scroll_result==False):
                                return
                            #scrollable_div1=self.driver.find_element(ByF.CLASS_NAME, self.divs_dict['2gis']['class_name']['additional_panel'])
                            #self.driver.execute_script("arguments[0].scrollTop = arguments[0].scrollHeight*0", scrollable_div)
                            organization_tags=self.driver.find_element(By.CLASS_NAME, self.divs_dict['2gis']['class_name']['additional_panel']).find_elements(By.CLASS_NAME, self.divs_dict['2gis']['class_name']['found_addresses_unpressed'])
                            print(len(organization_tags))
                            for organization_tag in organization_tags:
                                org_headers=organization_tag.find_elements(By.CLASS_NAME, self.divs_dict['2gis']['class_name']['org_header'])
                                #print(len(org_headers))
                                if(len(org_headers)>0):
                                    links1=org_headers[0].find_elements(By.TAG_NAME,'a')
                                    for link1 in links1:
                                        if('link.2gis' not in link1.get_attribute('href')):
                                            org_link=link1.get_attribute('href')
                                            org_name=link1.get_attribute('textContent')
                                            org_id=parse_id(org_link)
                                            mid_organizations_pd.loc[len(mid_organizations_pd)]=[address, house_name, found_address, coords, org_name, org_link, org_id, 1, hreff_code]
                                            #                                           ['original_address','found_place','found_address','coords','place_name','place_link','2gis_id','inside_original']
                            time.sleep(2)
                    if(re.sub(r'\d', '', link.get_attribute('textContent'))=='Фото'):
                        photos_number=re.findall(r'\d+', link.get_attribute('textContent'))
                        if(len(photos_number)==0):
                            photos_number=0
                        else:
                            photos_number=int(photos_number[0])
                        #print(f'photos_number: {photos_number}')
                        if(photos_number>0):
                            clicked=0
                            while(clicked==0):
                                try:
                                    link.click()
                                    time.sleep(2)
                                    clicked=1
                                except:
                                    scroll_right=self.driver.find_elements(By.CLASS_NAME, self.divs_dict['2gis']['class_name']['scroll_right'])
                                    if(len(scroll_right)>0):
                                        scroll_right[0].click()
                                        time.sleep(2)
                                    else:
                                        clicked=1
                            no_photos=self.driver.find_elements(By.CLASS_NAME, self.divs_dict['2gis']['class_name']['add_photos'])
                            no_photos=self.driver.find_elements(By.CLASS_NAME, self.divs_dict['2gis']['class_name']['add_photos'])
                            if(len(no_photos)>0):
                                continue
                            more_tags=self.driver.find_element(By.CLASS_NAME, self.divs_dict['2gis']['class_name']['additional_panel']).find_elements(By.CLASS_NAME,self.divs_dict['2gis']['class_name']['more_photos_types'])
                            if(len(more_tags)>0):
                                more_tags[-1].click()
                                time.sleep(2)
                            outside_tags=self.driver.find_elements(By.CLASS_NAME,self.divs_dict['2gis']['class_name']['photos_type'])
                            for outside_tag in outside_tags:
                                tag_text=re.sub(r'\d', '', outside_tag.get_attribute('textContent'))
                                if(tag_text=='Снаружи'):
                                    photos_number=int(re.findall(r'\d+', outside_tag.get_attribute('textContent'))[0])
                                    outside_tag.click()
                                    isinside=True
                                    time.sleep(2)
                            scrollable_div1=self.driver.find_element(By.CLASS_NAME, self.divs_dict['2gis']['class_name']['additional_panel']).find_element(By.CLASS_NAME, self.divs_dict['2gis']['class_name']['scrollable_div'])
                            scroll_result=self.scroll_down(scrollable_div1, self.divs_dict['2gis']['class_name']['photos_divs'], By.CLASS_NAME, photos_number)
                            if(scroll_result==False):
                                return
                            photo_divs=self.driver.find_elements(By.CLASS_NAME, self.divs_dict['2gis']['class_name']['photos_divs'])
                            print(f'photo divs: {len(photo_divs)}')
                            for photo_div in photo_divs:
                                photo_link=photo_div.get_attribute('style')
                                mid_photos_building_pd.loc[len(mid_photos_building_pd)]=[address, house_name, found_address, coords, hreff, photo_link, hreff_code, False]
                                #print(f'photo divs df: {len(mid_photos_building_pd)}')
                                #mid_organizations_pd.loc[len(mid_organizations_pd)]=[address, house_name,found_address, coords, found_name, hreff, hreff_code, 0]   
                                #                                                       ['original_address','found_place','found_address','coords', 'place_link', 'photo_link', '2gis_id', 'is_panorama']
                    #mid_place_parsed.loc[len(mid_place_parsed)]=[address, house_name, found_address, coords, found_name, hreff, hreff_code, 1]
                    #                                           ['original_address','found_place','found_address','coords','place_name','place_link','2gis_id','inside_original']
                self.save_organization_pd=pd.concat([self.save_organization_pd, mid_organizations_pd], ignore_index=True)        
                self.photos_building_pd=pd.concat([self.photos_building_pd, mid_photos_building_pd], ignore_index=True)                            
                mid_photos_building_pd=mid_photos_building_pd.loc[mid_photos_building_pd['original_address']=='$']
                mid_organizations_pd=mid_organizations_pd.loc[mid_organizations_pd['original_address']=='$']
                if(len(search_results)==1):
                    return 1
                
                #scroll_right_tag=self.driver.find_elements(By.CLASS_NAME, self.divs_dict['2gis']['class_name']['scroll_right'])
        else:
            return -3
              
                    
                            
        return -3
        
                    
                    
    
    #saves all files===========================================================
    def save_all(self, filename):
        if(len(self.photos_organization_pd)>0):
            self.save_photo_pd(filename)
        if(len(self.save_organization_pd)>0):
            self.save_org_pd(filename)
        if(len(self.photos_building_pd)>0):
            self.save_building_photos_pd(filename)
        if(len(self.photos_additional)>0):
            self.save_additional_photos_pd(filename)
        if(len(self.places_parsed)>0):
            self.places_parsed.to_csv(filename+'_links_saved.csv')

        
            
            
    #closes_driver==============================================================
    def close(self):
        self.driver.quit()
        
    def check_collected_list(self):
        print(self.already_taken)
        
        

#class ends======================================================



#downloads photos by links==================================================
def download_photos(filename, start=-1):
    cur_pd=pd.read_csv(filename)
    #cur_pd=cur_pd.sample(n=10)
    result_pd=pd.DataFrame(columns=['org_id', 'lon', 'lat', 'filename', 'filepath', 'exemplar'])
    k=-1
    for i, row in cur_pd.iterrows():
        k+=1
        #print(f'{k}/{len(cur_pd)}')
        if(k<=start):
            continue
        if(k%1000==0):
            print(f'{k}/{len(cur_pd)}')
        #time.sleep(1)
        #if(i>1000000):
        #    break
        photo_name=row['photo_link'].split('/')[-1].replace('?','!')
        #print(row['photo_link'])
        coords_part=str(row['lon'])+';'+str(row['lat'])
        response = requests.get(row['photo_link'], stream=True)
        #print(response)
        if(response.status_code==200):
            filepath='photos/2gis/'+coords_part+'/'+str(row['2gis_id'])+'/'
            total_filepath=filepath+photo_name
            #filepath='saved_files/'
            if(not(os.path.isdir(filepath))):
                os.makedirs(filepath)
            with open(total_filepath, 'wb') as out_file:
                shutil.copyfileobj(response.raw, out_file)
            del response
            result_pd.loc[len(result_pd)]=[row['2gis_id'], row['lon'], row['lat'], photo_name, total_filepath, 0]
            
            result_pd.to_csv('photos/2gis/photos_information.csv')
        else:
            print(row['photo_link'])
            print(response)
            

def download_photos_one_thread(filename, diap, index):
    print(f'{index}: thread_started')
    cur_pd=pd.read_csv(filename)
    cur_pd=cur_pd[diap[0]:diap[1]]
    #cur_pd=cur_pd.sample(n=10)
    #result_pd=pd.DataFrame(columns=['org_id', 'lon', 'lat', 'filename', 'filepath', 'exemplar'])
    k=-1
    for i, row in cur_pd.iterrows():
        k+=1
        #print(f'{k}/{len(cur_pd)}')
        if(k%1000==0):
            print(f'{index}: {k}/{len(cur_pd)}')
        if(os.path.isfile(row['filepath_found']) or 'api-version=2.0' in row['filepath_found']):
            continue
        else:
            photo_name=row['photo_link'].split('/')[-1].replace('?','!')
            response = requests.get(row['photo_link'], stream=True)
            #print(response)
            if(response.status_code==200):
                filepath='/'.join(row['filepath_found'].split('/')[:-1])
                total_filepath=row['filepath_found']
                #filepath='saved_files/'
                if(not(os.path.isdir(filepath))):
                    os.makedirs(filepath)
                with open(total_filepath, 'wb') as out_file:
                    shutil.copyfileobj(response.raw, out_file)
                del response
                #result_pd.loc[len(result_pd)]=[row['2gis_id'], row['lon'], row['lat'], photo_name, total_filepath, 0]
                
                #result_pd.to_csv('photos/2gis/photos_information.csv')
            else:
                print(row['photo_link'])
                print(response)
        
        
        
def download_photos_threads(filename, start, threads_num):
    len_cur_pd=len(pd.read_csv(filename))
    diaps=[]
    slicer=int((len_cur_pd-start)/threads_num)
    for i in range(threads_num+1):
        start_diap=start+i*slicer
        end_diap=start+(i+1)*slicer
        if(end_diap>len_cur_pd):
            end_len=len_cur_pd-1
        diaps.append([start_diap, end_diap])
    
    threads = [
            Thread(target=download_photos_one_thread, args=(filename, diaps[i] ,i ,))
            for i in range(len(diaps))
        ]
                    
    for thread in threads:
        thread.start()
    
    for thread in threads:
         thread.join() 
    
        
        
#remake=file=============================================================================================

def rebuild_file(filename):
    cur_pd=pd.read_csv(filename)
    #cur_pd=cur_pd.sample(n=10)
    result_pd=pd.DataFrame(columns=['org_id', 'lon', 'lat', 'outside', 'filename', 'filepath', 'exemplar'])
    k=-1
    for i, row in cur_pd.iterrows():
        k+=1
        photo_name=row['photo_link'].split('/')[-1].replace('?','!')
        print(row['photo_link'])
        coords_part=str(row['lon'])+';'+str(row['lat'])
        filepath='photos/2gis/'+coords_part+'/'+str(row['2gis_id'])+'/'
        total_filepath=filepath+photo_name
        if(os.path.exists(total_filepath)):
            print(total_filepath)
            result_pd.loc[len(result_pd)]=[row['2gis_id'], row['lon'], row['lat'], row['outside'], photo_name, total_filepath, 0]
        
    result_pd.to_csv('photos/2gis/photos_information.csv')


def test0(address):
    parser=Parser()
    parser.search_places_by_address(parser.base_URL, address)
    parser.save_all('test')
    #parser.close()



#parse=single address=====================================================
def test1(street, valuer):
    URL='https://2gis.ru/irkutsk'
    #dicter={'Лермонтова Улица':'Иркутск, улица Лермонтова, ','Старо-Кузьмихинская Улица':'Иркутск, Старо-Кузьмихинская улица, ',
    #        'Фаворского Улица':'Иркутск, улица Фаворского, ', 'Улан-Баторская Улица':'Иркутск, Улан-Баторская улица, '}
    #GET=LINKS================================================================
    
    parser=Parser()
    streets_df=pd.read_csv('streets_exploded_ungathered.csv')
    streets_df=streets_df.loc[streets_df['street']==street][valuer[0]:valuer[1]]
    for i, row in streets_df.iterrows():
        address=f'Иркутск, {street}, {row["house_intervals"]}'
        parser.search_places_by_address(URL, address)
    #parser.save_all(half_address+'_'+str(valuer[0])+'-'+str(valuer[1])+'_')
    parser.close()
    


#parse photos in cycle=================================    
def test2(filename):
    parser=Parser()
    organization_df=pd.read_csv(filename)
    for i, row in organization_df.iterrows():
        #self, URL, found_place, coords, org_name, address
        parser.parse_photos(row['place_link'], row['found_place'], row['coords'], row['place_name'], row['original_address'])
        if(i>3):
            break
    parser.save_all('test')
    parser.close()
    
    
def parse_places_test(half_address, streets, index):
    parser=Parser()
    for i,street in enumerate(streets):
            print(f'{index}:{street}; {i}/{len(streets)}')
        #try:
            parser.search_places_by_address(parser.base_URL, half_address+street)
        #except:
        #    print(f'{index}:ERROR')

    parser.save_all(half_address+'_'+str(index)+'_')
    parser.close()
    
    
def combine_files(filepath, subset_duplicate):
    total_filepath=os.path.abspath(os.curdir)+'/'+filepath
    filenames=os.listdir(total_filepath)
    print(filenames)
    result=None
    for i,filename in enumerate(filenames):
        if(filename=='total.csv' or filename[0]=='.'):
            continue
        if(os.path.isdir(total_filepath+'/'+filename)):
            continue
        if(i==0):
            result=pd.read_csv(total_filepath+'/'+filename)
        else:
            result=pd.concat([result, pd.read_csv(total_filepath+'/'+filename)])
            
    print(len(result))
    #while searching for orgs, keep all answers(even incorrect) to not parse them again. Return filter when compleatly filled
    result=result.loc[result['coords']!='-']
    
    if('coords' in result.columns):
        lons=[]
        lats=[]
        for coord in result['coords']:
            if(coord=='-'):
                lon, lat='-', '-'
            else:
                splitted=coord.split(', ')
                if(len(splitted)==1):
                    splitted=coord.split(',')
                lon=float(splitted[0])
                lat=float(splitted[1])
                if(lon<lat):
                    lon, lat=lat, lon
            lons.append(lon)
            lats.append(lat)
        result['lon']=lons
        result['lat']=lats
    
    if('place_link' in result.columns):
        #result.loc[result['place_link']]
        result=result[result['place_link']!='https://2gis.ru/irkutsk']
        result=result[result["place_link"].str.contains("/branches/")==False]
        place_ids=result['place_link'].tolist()
        for i in range(len(place_ids)):
            place_ids[i]=parse_id(place_ids[i])
            #print(len(place_ids[i]))
        result['2gis_id']=place_ids
    if('photo_link' in result.columns or 'image_url' in result.columns):
        if('photo_link' in result.columns):
            pls=result['photo_link'].tolist()
        else:
            pls=result['image_url'].tolist()
        for i in range(len(pls)):
            #print(pls[i])
            link_3="None"
            if(type(pls[i])==float):
                pls[i]='None'
                continue
            if(pls[i][0:6]=='https:'):
                #print('already parsed')
                continue
            link_1=re.findall(r'url\([^\)\,]+\)', pls[i])
            if(len(link_1)>0):
                link_2=re.findall(r'\".+\"', link_1[0])
                link_3=re.sub(r'\_\d+x\d+', '', link_2[0])
                link_3=re.sub(r'\"', '', link_3)
            pls[i]=link_3
        result['photo_link']=pls
        
        
    #\xa0
    result=result.drop_duplicates(subset=subset_duplicate)
    print(len(result))
    result.to_csv(total_filepath+'/total.csv', index=False)
    
    

def check_parsed(filepath):
    total_filepath=os.path.abspath(os.curdir)+'/'+filepath
    filenames=os.listdir(total_filepath)
    print(filenames)
    result=None
    for i,filename in enumerate(filenames):
        if(filename=='total.csv' or filename[0]=='.'):
            continue
        if(os.path.isdir(total_filepath+'/'+filename)):
            continue
        if(i==0):
            result=pd.read_csv(total_filepath+'/'+filename)
        else:
            result=pd.concat([result, pd.read_csv(total_filepath+'/'+filename)])
    
 
#parse many addresses=============================================
def test3(street):
    dicter={'Лермонтова Улица':'Иркутск, улица Лермонтова, ','Старо-Кузьмихинская Улица':'Иркутск, Старо-Кузьмихинская улица, '}
    streets_df=pd.read_csv('streets.csv')
    streets_df=streets_df.loc[streets_df['street']==street]
    sliced_streets=[]
    addition=0
    slice_peace=6
    for i in range(addition, addition+slice_peace):
        sliced_streets.append(streets_df['house_intervals'].values[i].split(','))
    half_address=dicter[street]
    print(sliced_streets)

    threads = [
            Thread(target=parse_places_test, args=(half_address, sliced_streets[i] ,i+addition ,))
            for i in range(slice_peace-addition)
        ]
                    
    for thread in threads:
        thread.start()
    
    for thread in threads:
         thread.join() 
         

def parse_orgs_by_address(sliced_df, diap, index):
    URL='https://2gis.ru/irkutsk'
    parser=Parser()
    counter1=0
    parse_continue=True
    parsed_counter=0
    counter=0
    for i, row in sliced_df.iterrows():
        print(f'{index}: {counter}/{len(sliced_df)}')
        counter+=1
        if(parse_continue==False):
            break
        if(counter%50==0):
            parser.save_all(f'Иркутск_rows_{str(diap[0])}-{str(diap[1])}_thread_{str(index)}')
            sliced_df.to_csv(f'stats_{str(diap[0])}-{str(diap[1])}_thread_{str(index)}.csv')
        try:
            stat=parser.search_places_by_address(URL, row['address'], True, index)
            print(f'thread {index}: stat is {stat} ({row["address"]})')
            sliced_df.at[i, '2gis_stat']=stat
        except Exception as exc_text:
            print(f'Error: thread {index}: {counter}/{len(sliced_df)}')
            with open('log_file.txt', 'a') as file:
                parser.save_all(f'Иркутск_rows_{str(diap[0])}-{str(diap[1])}_thread_{str(index)}')
                now = datetime.now()
                dt_string = now.strftime("%d/%m/%Y %H:%M:%S")
                file.write(f'{dt_string} Error: thread {index}: {counter}/{len(sliced_df)}; address: {row["address"]}\n {exc_text}')
                if('invalid session id' in str(exc_text)):
                    parse_continue=False
                if('session deleted as the browser has closed the connection' in str(exc_text)):
                    time.sleep(1)
                    parser.recreate_driver()
                
                #print(f'Иркутск, {street}, {house_name} finish')
    parser.save_all(f'Иркутск_rows_{str(diap[0])}-{str(diap[1])}_thread_{str(index)}')
    parser.close()
    sliced_df.to_csv(f'stats_{str(diap[0])}-{str(diap[1])}_thread_{str(index)}.csv')


def parse_organizations_threads(diap, threads_num):
    print('start')
    time_start=time.time()
    streets_pd = pd.read_csv('addresses_stats.csv')
    streets_pd=streets_pd.loc[streets_pd['2gis_stat']==-1]
    
    if(diap[1]>len(streets_pd)):
        diap[1]=len(streets_pd)
    streets_pd = streets_pd[diap[0]:diap[1]]
    print(diap[0], diap[1])
    if((diap[1]-diap[0])%threads_num!=0):
        return None
    slice_len=int((diap[1]-diap[0])/threads_num)
    threads = [
        Thread(target=parse_orgs_by_address, args=(
            streets_pd[i*slice_len:(i+1)*slice_len], diap, i,))
        for i in range(threads_num)
    ]

    for thread in threads:
        thread.start()

    for thread in threads:
        thread.join()
        
    print(f'total_time: {time.time()-time_start}')
    


def parse_photos_by_org(sliced_df, diap, index): 
    parser=Parser()
    counter1=0
    parse_continue=True
    parsed_counter=0
    counter=0
    for i, row in sliced_df.iterrows():
        time.sleep(5+round(random.uniform(0.5, 1), 2))
        print(f'{index}: {counter}/{len(sliced_df)}')
        counter+=1
        if(parse_continue==False):
            break
        if(counter%10==0):
            parser.save_all(f'Иркутск_rows_{str(diap[0])}-{str(diap[1])}_thread_{str(index)}_orgphotos')
            sliced_df.to_csv(f'stats_{str(diap[0])}-{str(diap[1])}_thread_{str(index)}_orgphotos.csv')
        try:
            stat=parser.parse_photos(row['place_link'], row['found_place'], row['coords'], row['found_place'], row['original_address'], row['2gis_id'],  index)
            #(self, URL, found_place, coords, org_name, address, dgis_id):
            #parser.parse_photos(URL, found_place, coords, org_name, address, dgis_id)
            print(f'thread {index}: stat is {stat} ({row["found_place"]})')
            sliced_df.at[i, '2gis_stat']=stat
        except Exception as exc_text:
            parser.save_all(f'Иркутск_rows_{str(diap[0])}-{str(diap[1])}_thread_{str(index)}_orgphotos')
            print(f'Error: thread {index}: {counter}/{len(sliced_df)}')
            with open('log_file.txt', 'a') as file:
                now = datetime.now()
                dt_string = now.strftime("%d/%m/%Y %H:%M:%S")
                file.write(f'{dt_string} Error: thread {index}: {counter}/{len(sliced_df)}; address: {row["found_place"]}\n {exc_text}')
                if('invalid session id' in str(exc_text)):
                    parse_continue=False
                if('Timed out receiving message' in str(exc_text)):
                    print('TIMEOUT')
                    parse_continue=False
                
                #print(f'Иркутск, {street}, {house_name} finish')
    print(f"saving: {index}")
    parser.save_all(f'Иркутск_rows_{str(diap[0])}-{str(diap[1])}_thread_{str(index)}_orgphotos')
    parser.close()
    sliced_df.to_csv(f'stats_{str(diap[0])}-{str(diap[1])}_thread_{str(index)}_orgphotos.csv')
    

def parse_photos_orgs_threads(diap, threads_num):
    time_start=time.time()
    streets_pd = pd.read_csv('addresses_stats_2gis_orgs.csv')
    streets_pd=streets_pd[streets_pd['2gis_stat']==0]
    if(diap[1]>len(streets_pd)):
        diap[1]=len(streets_pd)
    streets_pd = streets_pd[diap[0]:diap[1]]
    if((diap[1]-diap[0])%threads_num!=0):
        return None
    slice_len=int((diap[1]-diap[0])/threads_num)
    threads = [
        Thread(target=parse_photos_by_org, args=(
            streets_pd[i*slice_len:(i+1)*slice_len], diap, i,))
        for i in range(threads_num)
    ]

    for thread in threads:
        thread.start()

    for thread in threads:
        thread.join()
        
    print(f'total_time: {time.time()-time_start}')

         

def check_parsed():
    parser=Parser()
    parser.recheck_photos()
    parser.save_all('Академгородок_')
    parser.close()
    
def delete_duplicates_addtitonals(filename1, filename2, column_name):
    first_pd=pd.read_csv(filename1)
    second_pd=pd.read_csv(filename2)
    checker=second_pd[column_name].tolist()
    for check in checker:
        first_pd=first_pd.loc[first_pd[column_name]!=check]
    first_pd.to_csv(filename1.replace('.csv','_cleared.csv'))
    

def compare_addresses():
    change_dict={'1-й Берег Ангары Улица':'улица Берег Ангары', "1-й Ленинградский Переулок":"Ленинградский переулок",
                       '1-ый Ленинский квартал Территория':'Первый Ленинский Квартал'}
    
    kladr_streets=pd.read_csv('streets_exploded.csv')
    total_orgs=pd.read_csv('gathered_data_2/2gis/organizations/total.csv')
    gathered_links=set(total_orgs['original_address'].tolist())
    print(f'Total number of streets: {len(kladr_streets)}')
    counter=0
    for i, row in kladr_streets.iterrows():
        if(row['street'] in change_dict.keys()):
            streeter=change_dict[row['street']]
        else:
            streeter=row['street']
        final_address=f'Иркутск, {streeter}, {row["house_intervals"]}'
        if(final_address in gathered_links):
            counter+=1
    print(f'Gathered links: {counter}')
    print(f'Ungathered links: {len(kladr_streets)-counter}')
    

def clear_saved_files(filename_base):
    filenames=os.listdir(os.path.abspath(os.curdir))
    files_dict={}
    for filename in filenames:
        if(filename_base in filename):
            matches_number=re.finditer(r'\d+', filename)
            last_match=[0,1]
            for match_number in matches_number:
                if(match_number.start()>last_match[0]):
                    last_match=[match_number.start(), match_number.end()]
            filename_number=int(filename[last_match[0]:last_match[1]])
            filename_without_number=filename[0:last_match[0]]
            if(filename_without_number in files_dict.keys()):
                if(filename_number>files_dict[filename_without_number]['number']):
                    files_dict[filename_without_number]['number']=filename_number
                    files_dict[filename_without_number]['filename']=filename
            else:
                files_dict[filename_without_number]={'number': filename_number, 'filename':filename}
                
    correct_files=[files_dict[key]['filename'] for key in files_dict.keys()]
    for filename in filenames:
        if(filename_base in filename and filename not in correct_files):
            print(filename)
            os.remove(filename)
                
            #last_number=matches_number[0]
            #print(f'{matcher.start()}-{matcher.end()}')
            #print(f"Found '{matcher.group()}' at position {matcher.start()}-{matcher.end()}")
            #filename_without_number=filename
            

def rebuild_stat_file(org_photos=False):
    if(org_photos==False):
        base_stat=pd.read_csv('addresses_stats.csv')
        output_name='addresses_stats_test.csv'
        id_name='id'
    else:
        base_stat=pd.read_csv('addresses_stats_2gis_orgs.csv')
        output_name='addresses_stats_2gis_orgs_test.csv'
        id_name='2gis_id'
    print(base_stat.columns)
    filenames=os.listdir(os.path.abspath(os.curdir))
    for filename in filenames:
        if(filename[0]!='.' and 'stat' in filename and '.csv' in filename and filename not in ['addresses_stats.csv', 'addresses_stats_2gis_orgs.csv']):
            print(filename)
            add_pd=pd.read_csv(filename)
            ids=add_pd[id_name].tolist()
            stats=add_pd['2gis_stat'].tolist()
            for i in range(len(add_pd)):
                if(stats[i]!=0):
                    base_stat.loc[base_stat[id_name]==ids[i], ['2gis_stat']]=stats[i]
                    
    base_stat.to_csv(output_name, index=False)
    
    

def count_uncollected(filebase):
    filenames=os.listdir(os.path.abspath(os.curdir))
    zcount=0
    for filename in filenames:
        if(filebase in filename):
            df=pd.read_csv(filename)
            zcount+=len(df[df['2gis_stat']==0])
    print(zcount)
    
    
def make_super_total_files(filepath): 
    orgphotos=pd.read_csv(filepath+'/total_org_photos.csv')
    buildphotos=pd.read_csv(filepath+'/total_building_photos.csv')
    organizations=pd.read_csv(filepath+'/total_organizations.csv')
    photos_total=pd.DataFrame({'photo_url':[], 'filepath':[]})
    photos_total_url=[]
    photos_total_filepath=[]
    
    for i, row in buildphotos.iterrows():
        building=organizations[organizations['2gis_id']==row['2gis_id']]
        if(building['inside_original'].tolist()[0]==0):
            dirpath=f"photos/2gis/{building['original_address'].tolist()[0]}"
        else:
            original_building=organizations[organizations['2gis_id']==building['original_for_inside'].tolist()[0]]['original_address'].tolist()[0]
            dirpath=f"photos/2gis/{original_building}"
        photos_total_url.append(row['photo_link'])
        photos_total_filepath.append(dirpath)
    
    for i, row in orgphotos.iterrows():
        building=organizations[organizations['2gis_id']==row['2gis_id']]
        if(building['inside_original'].tolist()[0]==0):
            dirpath=f"photos/2gis/{building['original_address'].tolist()[0]}"
        else:
            original_building=organizations[organizations['2gis_id']==building['original_for_inside'].tolist()[0]]['original_address'].tolist()[0]
            dirpath=f"photos/2gis/{original_building}"
        photos_total_url.append(row['photo_link'])
        photos_total_filepath.append(dirpath)
        
    photos_total['photo_url']=photos_total_url
    photos_total['filepath']=photos_total_filepath
    photos_total=photos_total.drop_duplicates(subset=['photo_url'])
    print(len(photos_total))
    photos_total.to_csv(f"{filepath}/all_photos_2.csv")
    
    '''
    for i, row in organizations.iterrows():
        if(row['inside_original']==0):
            dirpath=f"photos/2gis/{row['original_address']}"
        else:
            original_building=organizations[organizations['2gis_id']==row['original_for_inside']]['original_address'].tolist()[0]
            dirpath=f"photos/2gis/{original_building}"
        print(dirpath)
        
        if(i>100):
            break     
    '''           


def build_photos_links(filepath='gathered_data/2gis/results'):
    orgphotos=pd.read_csv(filepath+'/total_org_photos.csv')
    buildphotos=pd.read_csv(filepath+'/total_building_photos.csv')
    organizations=pd.read_csv(filepath+'/total_organizations.csv')
    filepaths_found=[]
    filepaths_original=[]
    filepaths_coords=[]
    lons=[]
    lats=[]
    #photos_links=[]
    print(buildphotos.columns)
    file=open('2gis_address_check.txt','w')
    for i, row in buildphotos.iterrows():
        found_addr='ERROR'
        lat=re.findall(r'\d+\.\d+', row['coords'])[0]
        lon=re.findall(r'\d+\.\d+', row['coords'])[1]
        if(float(lon)<float(lat)):
            lon, lat=lat, lon
        #print(row['found_address'])
        #print(len(re.findall(r',\s\d', row['found_address'])))
        if(len(re.findall(r'\,\s\d', row['found_address']))>0 or len(re.findall(r'\,\sстр\d',row['found_address']))>0):
            found_addr=row['found_address']
        else:
            if(len(re.findall(r'\,\s\d',row['found_place']))>0 or len(re.findall(r'\,\sстр\d',row['found_place']))>0):
                found_addr=row['found_place']
        photo_name=row['photo_link'].split('/')[-1].replace('?','!')
        found_addr=found_addr.replace('/','_').replace('?','!').replace('\u200b',' ').replace('\xa0',' ')
        filepaths_original.append(f"photos_original/Иркутск, {row['original_address'].replace('/','_').replace('?','!')}/{photo_name}")
        filepaths_found.append(f"photos/2gis/Иркутск, {found_addr}/{photo_name}")
        filepaths_coords.append(f"photos_coords_upd/2gis/{lon}_{lat}/{photo_name}")
        lons.append(lon)
        lats.append(lat)
    buildphotos['filepath_original']=filepaths_original
    buildphotos['filepath_found']=filepaths_found
    buildphotos['filepath_coords']=filepaths_coords
    buildphotos['longitude']=lons
    buildphotos['latitude']=lats
    buildphotos=buildphotos.filter(['filepath_original', 'filepath_found', 'filepath_coords', 'original_address', 'found_address', 'found_name', 'photo_link','2gis_id', 'longitude', 'latitude'])


    
    filepaths_found=[]
    filepaths_original=[]
    filepaths_coords=[]
    lons=[]
    lats=[]
    for i, row in orgphotos.iterrows():
        found_addr='ERROR'
        lat=re.findall(r'\d+\.\d+', row['coords'])[0]
        lon=re.findall(r'\d+\.\d+', row['coords'])[1]
        if(float(lon)<float(lat)):
            lon, lat=lat, lon
        photo_name=row['photo_link'].split('/')[-1].replace('?','!')
        #filepaths_original.append(f"photos_original/Иркутск, {row['original_address'].replace('/','_').replace('?','!')}/{photo_name}")
        found_plc=organizations[organizations['2gis_id']==row['2gis_id']]['found_place'].tolist()[0]
        found_addr1=organizations[organizations['2gis_id']==row['2gis_id']]['found_address'].tolist()[0]
        #print(f"{found_plc}; {found_addr}")
        if(len(re.findall(r'\,\s\d', found_addr1))>0 or len(re.findall(r'\,\sстр\d', found_addr1))>0):
            found_addr=found_addr1
        else:
            if(len(re.findall(r'\,\s\d', found_plc))>0 or len(re.findall(r'\,\sстр\d', found_plc))>0):
                found_addr=found_plc

        found_addr=found_addr.replace('/','_').replace('?','!').replace('\u200b',' ').replace('\xa0',' ')
        filepaths_original.append(f"photos_original/Иркутск, {row['original_address'].replace('/','_').replace('?','!')}/{photo_name}")
        filepaths_found.append(f"photos/2gis/Иркутск, {found_addr}/{photo_name}")
        filepaths_coords.append(f"photos_coords_upd/2gis/{lon}_{lat}/{photo_name}")
        lons.append(lon)
        lats.append(lat)
    orgphotos['filepath_original']=filepaths_original
    orgphotos['filepath_found']=filepaths_found
    orgphotos['filepath_coords']=filepaths_coords
    orgphotos['longitude']=lons
    orgphotos['latitude']=lats
    orgphotos=orgphotos.filter(['filepath_original', 'filepath_found', 'filepath_coords', 'original_address', 'found_address', 'found_name', 'photo_link','2gis_id', 'longitude', 'latitude'])

    res=pd.concat([buildphotos, orgphotos])
    res.to_csv(filepath+'/all_photos_all.csv', index=False)
        
    

        
    

if __name__=='__main__':
    #parse_photos_orgs_threads([0, 100], 1)
    #parse_organizations_threads([0,200], 1)
    #clear_saved_files('Иркутск_rows_0-85')
    #rebuild_stat_file(org_photos=True)
    #count_uncollected('stats_0-2800')
    
    #df=pd.read_csv('addresses_stats_2gis_orgs.csv')
    #print(len(df[df['2gis_stat']==0]))
    #combine_files('gathered_data/2gis/organizations', ['2gis_id'])
    #combine_files('gathered_data/2gis/building_photos', ['photo_link'])
    #combine_files('gathered_data/2gis/organization_photos', ['photo_link'])
    
    #make_super_total_files('gathered_data/2gis/results')
    #download_photos_threads('gathered_data/2gis/results/all_photos_all.csv', 0, 1)
    build_photos_links()
    pass
    


    
    
    