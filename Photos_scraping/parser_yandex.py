import undetected_chromedriver as uc
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.action_chains import ActionChains
from webdriver_manager.chrome import ChromeDriverManager
from selenium import webdriver
from threading import Thread
import requests
import shutil
import json
import time
import random
import math
import pandas as pd
import numpy as np
import re
import os
from datetime import datetime


def json_read(filename: str):
    with open(filename) as f_in:
        return json.load(f_in)


def remove_last(stroka):
    stroka = re.sub(r'\?ll=\d+\.\d+\%2C\d+\.\d+\&z=\d+\.\d+', '', stroka)
    return stroka


def get_id(stroka):
    ider = re.findall(r'\/\d+\/', stroka)
    if (len(ider) > 0 and len(ider[0]) > 10):
        return ider[0].replace('/', '')
    else:
        return stroka


class Parser():

    def __init__(self):
        self.already_taken = []
        
        self.save_organization_pd = pd.DataFrame(
            columns=['original_address', 'found_place', 'found_address', 'coords', 'place_name', 'place_link', 'ya_id', 'inside_original', 'original_link'])
        self.photos_organization_pd = pd.DataFrame(
            columns=['original_address', 'found_address', 'place_name', 'place_link', 'coords', 'photo_link', 'inside', 'ya_id', 'supposed_number_of_photos'])
        self.photos_building_pd = pd.DataFrame(
            columns=['original_address', 'found_place', 'found_address', 'coords', 'place_link', 'photo_link', 'ya_id', 'is_panorama'])
        #cService = webdriver.ChromeService(executable_path='./chromedriver_136')
        #self.driver=webdriver.Chrome(service = cService)
        
        driver_exec_path = ChromeDriverManager().install()
        self.driver = uc.Chrome(driver_executable_path=driver_exec_path)
        
        self.divs_dict = json_read('tags.json')
        self.clicked_hrefs=[]
        self.wrong_places=['Иркутск']
        self.base_URL = 'https://yandex.ru/maps/63/irkutsk/?ll=104.280608%2C52.289590&z=12'
        self.wrong_places=['Иркутск','Падь Мельничная','река Ангара']
        self.wrong_types=['СНТ','СПК', 'Некоммерческое СТ', 'некоммерческое СТ', 'Садоводческое товарищество', 'Садоводческий кооператив',
                     'садоводческого кооператива','Садоводческое товарищество','ДНТ']
        self.status_codes={'many_variants':-4,'unclear_warning':-3,'wrong_type':-2,'nothing_found':-1,'not_collected': 0, 'collected':1, 'error_in_code':-3}
        
        
    def recreate_driver(self):
        cService = webdriver.ChromeService(executable_path='./chromedriver_136')
        self.driver=webdriver.Chrome(service = cService)

    # saves file with organizations link==============================================

    def save_org_pd(self, base_name=None):
        if (base_name == None):
            base_name = 'yandex_org_links'
        else:
            base_name = base_name+'_yandex_org_links'
        number = 0
        filenames = os.listdir(os.path.abspath(os.curdir))
        for filename in filenames:
            if (base_name in filename):
                numbers = re.findall(r'\d+', filename)
                if (len(numbers) > 0 and int(numbers[-1]) > number):
                    number = int(numbers[-1])
        number += 1
        self.save_organization_pd.to_csv(base_name+str(number)+'.csv')

    # saves file with photos link================================================

    def save_photo_pd(self, base_name=None):
        if (base_name == None):
            base_name = 'yandex_photos_links'
        else:
            base_name = base_name+'_yandex_photos_links'
        number = 0
        filenames = os.listdir(os.path.abspath(os.curdir))
        for filename in filenames:
            if (base_name in filename):
                numbers = re.findall(r'\d+', filename)
                if (len(numbers) > 0 and int(numbers[-1]) > number):
                    number = int(numbers[-1])
        number += 1
        self.photos_organization_pd.to_csv(base_name+str(number)+'.csv')

    # saves file with photos of building itself====================================

    def save_building_photos_pd(self, base_name=None):
        if (base_name == None):
            base_name = 'yandex_photos_building_links'
        else:
            base_name = base_name+'_yandex_photos_building_links'
        number = 0
        filenames = os.listdir(os.path.abspath(os.curdir))
        for filename in filenames:
            if (base_name in filename):
                numbers = re.findall(r'\d+', filename)
                if (len(numbers) > 0 and int(numbers[-1]) > number):
                    number = int(numbers[-1])
        number += 1
        self.photos_building_pd.to_csv(base_name+str(number)+'.csv')

    def save_additional_photos_pd(self, base_name=None):
        if (base_name == None):
            base_name = 'yandex_photos_additional_links'
        else:
            base_name = base_name+'_yandex_photos_additional_links'
        number = 0
        filenames = os.listdir(os.path.abspath(os.curdir))
        for filename in filenames:
            if (base_name in filename):
                numbers = re.findall(r'\d+', filename)
                if (len(numbers) > 0 and int(numbers[-1]) > number):
                    number = int(numbers[-1])
        number += 1
        self.photos_additional.to_csv(base_name+str(number)+'.csv')

    # checks if the page was moved to yandex uncapha===============================

    def check_limited(self):
        limiteds = self.driver.find_elements(By.TAG_NAME, 'pre')
        if (len(limiteds) > 0):
            for limited in limiteds:
                if (limited.get_attribute('textContent') == 'limited'):
                    return True
        return False

    # scrolling down until no updates on page

    def scroll_down(self, scrollable_div, list_element_name, list_element_key, min_elem_count=0, counter=3, forced_rescroll=True):
        new_len = len(self.driver.find_elements(
            list_element_key, list_element_name))
        old_len = 0
        def_count = 0
        less_than_current = 0
        rescroll_counter = 0
        while (def_count < counter):
            print(new_len)
            command = "arguments[0].scrollTop = arguments[0].scrollHeight"
            self.driver.execute_script(command, scrollable_div)
            time.sleep(round(random.uniform(0.5, 1), 2))
            new_len = len(self.driver.find_elements(
                list_element_key, list_element_name))
            if (new_len >= min_elem_count):
                break
            if (new_len == old_len):
                def_count += 1
            else:
                old_len = new_len
            if(rescroll_counter>10):
                return False
            if (def_count >= counter and new_len < min_elem_count):
                def_count = 0
                rescroll_counter+=1
                print('rescroll')
                for i in range(100):
                    command = f"arguments[0].scrollTop = arguments[0].scrollHeight*{i}/{100}"
                    self.driver.execute_script(command, scrollable_div)
                    time.sleep(0.1)
        if (forced_rescroll == True and min_elem_count>20):
            for i in range(100):
                command = f"arguments[0].scrollTop = arguments[0].scrollHeight*{i}/{100}"
                self.driver.execute_script(command, scrollable_div)
                time.sleep(0.1)
        return True

    # get all links on photos of organization=======================================

    def parse_photos(self, row):
        mid_photos_organization_pd = pd.DataFrame(
            columns=['original_address', 'found_address', 'place_name', 'place_link', 'coords', 'photo_link', 'inside', 'ya_id', 'supposed_number_of_photos'])
        #                                       columns=['original_address','found_address','place_name','place_link','coords','photo_link','inside','ya_id', 'supposed_number_of_photos']
        self.driver.get(row['place_link'])
        time.sleep(2+round(random.uniform(0, 1), 2))
        
        carusel_content_divs=self.driver.find_elements(By.CLASS_NAME, self.divs_dict['yandex']['class_name']['carousel_content'])
        error_tabs=self.driver.find_elements(By.CLASS_NAME, self.divs_dict['yandex']['class_name']['error_tab'])
        while(len(carusel_content_divs)==0 and len(error_tabs)==0):
            time.sleep(0.1)
            error_tabs=self.driver.find_elements(By.CLASS_NAME, self.divs_dict['yandex']['class_name']['error_tab'])
            carusel_content_divs=self.driver.find_elements(By.CLASS_NAME, self.divs_dict['yandex']['class_name']['carousel_content'])
        
        if(len(error_tabs)>0):
            return -2
        
        photos_divs = []
        photos_button = self.driver.find_elements(By.XPATH, self.divs_dict['yandex']['xpath']['photos_button'])
        if (len(photos_button) > 0):
            photos_number = re.findall(r'\d+', photos_button[0].get_attribute('textContent'))
            if (len(photos_number) > 0):
                photos_number = int(photos_number[0])
            else:
                photos_number = 0
            photos_button[0].click()
            time.sleep(3+round(random.uniform(0, 1), 2))
            buttons = self.driver.find_elements(By.CLASS_NAME, self.divs_dict['yandex']['class_name']['photos_tab'])
            print(f'len buttons:{len(buttons)}')
            isinside = False
            for button in buttons:
                #print(button.get_attribute('textContent'))
                if (button.get_attribute('textContent') == 'Снаружи'):
                    button_clicked = False
                    while (button_clicked == False):
                        try:
                            button.click()
                            button_clicked = True
                            isinside = True
                        except:
                            arrows = self.driver.find_elements(By.XPATH, self.divs_dict['yandex']['xpath']['arrow_move'])
                            if (len(arrows) > 0):
                                arrows[-1].click()
                                time.sleep(0.5)

                if (isinside == True):
                    break
            time.sleep(1+round(random.uniform(0, 3), 2))
            photos_divs = self.driver.find_elements(By.XPATH, self.divs_dict['yandex']['xpath']['image_photo'])

            scrollable_div = self.driver.find_elements(By.CLASS_NAME, self.divs_dict['yandex']['class_name']['scroll_container'])[0]
            old_len = 0
            self.scroll_down(scrollable_div, self.divs_dict['yandex']['xpath']['image_photo'], By.XPATH)
            print(len(photos_divs))

        if (len(photos_divs) > 0):
            for photo in photos_divs:
                photo_link = photo.get_attribute('src')
                # print(photo_link)
                mid_photos_organization_pd.loc[len(mid_photos_organization_pd)] = [row['original_address'], row['found_address'],
                                                                                   row['place_name'], row['place_link'], row['coords'], photo_link, isinside, row['ya_id'], photos_number]
            self.photos_organization_pd = pd.concat([self.photos_organization_pd, mid_photos_organization_pd], ignore_index=True)
            #                                       columns=['original_address','found_address','place_name','place_link','coords','photo_link','inside','ya_id', 'supposed_number_of_photos']
            return 1
        else:
            return -1

    # parses places organizations inside builing with given address=================

    def search_places_by_address(self, URL, address, download_photos=True):
        for wrong_type in self.wrong_types:
            if(wrong_type in address):
                return -2
        input_tag = self.driver.find_elements(By.TAG_NAME, self.divs_dict['yandex']['tag_name']['input_line'])
        if (len(input_tag) == 0 or (len(input_tag)>0 and input_tag[0].get_attribute('placeholder')!='Поиск и выбор мест')):
            self.driver.get(URL)
            time.sleep(2+round(random.uniform(0, 1), 2))
        
        mid_organizations_pd = pd.DataFrame(columns=['original_address', 'found_place', 'found_address',
                                            'coords', 'place_name', 'place_link', 'ya_id', 'inside_original', 'original_link'])
        #                                   columns=['original_address','found_place','found_address','coords','place_name','place_link','ya_id','inside_original', 'original_link']


        input_tag = self.driver.find_elements(By.TAG_NAME, self.divs_dict['yandex']['tag_name']['input_line'])
        if (len(input_tag) > 0):
            input_tag = input_tag[0]
            time.sleep(0.5)
            input_tag.send_keys(Keys.CONTROL + "a")
            time.sleep(0.5)
            input_tag.send_keys(Keys.DELETE)
            time.sleep(0.5)
            input_tag.send_keys(address)
            time.sleep(1+round(random.uniform(0, 3), 2))

            input_tag.send_keys(Keys.ENTER)
            #time.sleep(3+round(random.uniform(0, 3), 2))
            
            org_name_len=self.driver.find_elements(By.CLASS_NAME, self.divs_dict['yandex']['class_name']['organization_name'])
            nothing_found=self.driver.find_elements(By.CLASS_NAME, self.divs_dict['yandex']['class_name']['nothing_found'])
            card_title=self.driver.find_elements(By.CLASS_NAME, self.divs_dict['yandex']['class_name']['card_title'])
            unclear_warning=self.driver.find_elements(By.CLASS_NAME, self.divs_dict['yandex']['class_name']['unclear_warning'])
            while(len(org_name_len)==0 and len(nothing_found)==0 and len(card_title)==0 and len(unclear_warning)==0):
                time.sleep(0.1)
                org_name_len=self.driver.find_elements(By.CLASS_NAME, self.divs_dict['yandex']['class_name']['organization_name'])
                nothing_found=self.driver.find_elements(By.CLASS_NAME, self.divs_dict['yandex']['class_name']['nothing_found'])
                card_title=self.driver.find_elements(By.CLASS_NAME, self.divs_dict['yandex']['class_name']['card_title'])
                unclear_warning=self.driver.find_elements(By.CLASS_NAME, self.divs_dict['yandex']['class_name']['unclear_warning'])
            
            time.sleep(1+round(random.uniform(0, 3), 2))
            coords = "-"
            
            places_list=self.driver.find_elements(By.CLASS_NAME, 'search-list-view__list')
            if(len(places_list)>0):
                orgs_in_list=self.driver.find_elements(By.TAG_NAME, 'li')
                first_links=orgs_in_list[0].find_elements(By.TAG_NAME, 'a')
                if(len(first_links)>0):
                    first_place_link=first_links[0].get_attribute('href')
                    self.driver.get(first_place_link)
                    time.sleep(5+round(random.uniform(0, 1), 2))
                else:
                    orgs_in_list[0].click()
                    time.sleep(0.3)
                
                #orgs_in_list[0].click()
                #time.sleep(0.4)
                
            
            
            if(len(nothing_found)>0):
                return -1
            
            #if(len(unclear_warning)>0):
            #    return -3
            
            if(len(nothing_found)==0):
                
                #found_places=self.driver.find_elements(By.XPATH, self.divs_dict['yandex']['xpath']['organization_list_element_div'])
                
                coords_div = self.driver.find_elements(By.CLASS_NAME, self.divs_dict['yandex']['class_name']['coords_elem'])
                if (len(coords_div) > 0):
                    coords = coords_div[0].get_attribute('textContent')
                else:
                    coords_div = self.driver.find_elements(By.XPATH, self.divs_dict['yandex']['xpath']['marker_div'])
                    if (len(coords_div) > 0):
                        coords = coords_div[0].get_attribute('data-coordinates')
    
                cur_url = self.driver.current_url
    
                card_title_div = self.driver.find_elements(By.CLASS_NAME, self.divs_dict['yandex']['class_name']['card_title'])
                if(len(card_title_div)>0):
                    place_name = card_title_div[0].get_attribute('textContent')
                else:
                    card_title_div=self.driver.find_elements(By.CLASS_NAME, 'orgpage-header-view__header')
                    if(len(card_title_div)>0):
                        place_name = card_title_div[0].get_attribute('textContent')
                    else:
                        place_name='error'
                    
                
                #if(cur_url in self.clicked_hrefs or cur_url in self.already_taken or place_name in self.wrong_places):
                if(place_name in self.wrong_places):
                    return -1
    
                place_description = self.driver.find_elements(By.CLASS_NAME, self.divs_dict['yandex']['class_name']['card_description'])
                if(len(place_description)>0):
                    found_address = place_description[0].get_attribute('textContent')
                else:
                    address_description = self.driver.find_elements(By.CLASS_NAME, self.divs_dict['yandex']['class_name']['address_div'])
                    if(len(address_description)>0):
                        found_address=address_description[0].get_attribute('textContent')
                    else:
                        found_address='None'
    
                mid_organizations_pd.loc[len(mid_organizations_pd)] = [
                    address, place_name, found_address, coords, place_name, cur_url, 'None', 0, cur_url]
                #                                           columns=['original_address','found_place','found_address','coords','place_name','place_link','ya_id','inside_original', 'original_link']
    
                if (download_photos == True):
                    mid_photos_building_pd = pd.DataFrame(columns=['original_address', 'found_place', 'found_address', 'coords', 'place_link', 'photo_link', 'ya_id', 'is_panorama'])
                    photos_number_divs = self.driver.find_elements(By.CLASS_NAME, self.divs_dict['yandex']['class_name']['photos_number'])
                    photos_number=0
                    if (len(photos_number_divs) > 0):
                        for photos_number_div in photos_number_divs:
                            if ('фото' in photos_number_div.get_attribute('textContent') and len(re.findall(r'\d+', photos_number_div.get_attribute('textContent'))) > 0):
                                photos_number = int(re.findall(r'\d+', photos_number_div.get_attribute('textContent'))[0])
                    else:
                        photos_number = 0
                    #print(photos_number)
                    main_photos_button = self.driver.find_elements(By.XPATH, self.divs_dict['yandex']['xpath']['open_photos_main_page'])
                    if (len(main_photos_button) > 0):
                        main_photos_button[0].click()
                        #print('clicked')
                        time.sleep(3+round(random.uniform(0, 1), 2))
                        #print('waitied')
                        photo_container = self.driver.find_elements(By.CLASS_NAME, self.divs_dict['yandex']['class_name']['photos_container'])
                        if(len(photo_container)==0):
                            photo_container=self.driver.find_elements(By.CLASS_NAME, self.divs_dict['yandex']['class_name']['photos_single_container'])
                        #print(f"len(photo_container): {len(photo_container)}")
                        if (len(photo_container) > 0):
                            if(len(photo_container[0].find_elements(By.CLASS_NAME, self.divs_dict['yandex']['class_name']['scroll_container']))>0):
                                scrollable_div = photo_container[0].find_elements(By.CLASS_NAME, self.divs_dict['yandex']['class_name']['scroll_container'])[0]
                                self.scroll_down(scrollable_div, self.divs_dict['yandex']['xpath']['image_photo'], By.XPATH, photos_number)
                            photos_divs = self.driver.find_elements(By.XPATH, self.divs_dict['yandex']['xpath']['image_photo'])
                            #print(f'main photos div length: {len(photos_divs)}')
                            for photo_div in photos_divs:
                                photo_link = photo_div.get_attribute('src')
                                mid_photos_building_pd.loc[len(mid_photos_building_pd)] = [
                                    address, place_name, found_address, coords, cur_url, photo_link, 'None', 0]
                                #                                               (columns=['original_address','found_place','found_address','coords', 'place_link', 'photo_link', 'ya_id', 'is_panorama'])
                            main_photos_button_close = self.driver.find_elements(By.XPATH, self.divs_dict['yandex']['xpath']['close_photos_main_page'])
                            while(len(main_photos_button_close)==0):
                                time.sleep(0.1)
                                main_photos_button_close = self.driver.find_elements(By.XPATH, self.divs_dict['yandex']['xpath']['close_photos_main_page'])
                            main_photos_button_close[0].click()
                            #print('close_clicked')
                            time.sleep(2+round(random.uniform(0, 1), 2))
                    panorama_div = self.driver.find_elements(By.XPATH, self.divs_dict['yandex']['xpath']['panorama_but'])
                    #print(len(panorama_div))
                    if (len(panorama_div) > 0):
                        panorama_links = panorama_div[0].find_elements(By.TAG_NAME, 'a')
                        if (len(panorama_links) > 0):
                            mid_photos_building_pd.loc[len(mid_photos_building_pd)] = [
                                address, place_name, found_address, coords, cur_url, panorama_links[0].get_attribute('href'), 'None', 1]
                    self.photos_building_pd = pd.concat([self.photos_building_pd, mid_photos_building_pd], ignore_index=True)
                    photos_button = self.driver.find_elements(By.XPATH, self.divs_dict['yandex']['xpath']['photos_button'])
                    if (len(photos_button) > 0):
                        photos_number = re.findall(r'\d+', photos_button[0].get_attribute('textContent'))
                        if (len(photos_number) > 0):
                            photos_number = int(photos_number[0])
                        else:
                            photos_number = 0
                        photos_button[0].click()
                        time.sleep(3+round(random.uniform(0, 1), 2))
                        buttons = self.driver.find_elements(
                            By.XPATH, self.divs_dict['yandex']['xpath']['button1'])
                        isinside = False
                        for button in buttons:
                            if (button.get_attribute('textContent') == 'Снаружи'):
                                button_clicked = False
                                while (button_clicked == False):
                                    try:
                                        button.click()
                                        button_clicked = True
                                        isinside = True
                                    except:
                                        arrows = self.driver.find_elements(By.XPATH, self.divs_dict['yandex']['xpath']['arrow_move'])
                                        if (len(arrows) > 0):
                                            arrows[-1].click()
                                            time.sleep(0.5)

                            if (isinside == True):
                                break
                        time.sleep(1+round(random.uniform(0, 3), 2))
                        photos_divs = self.driver.find_elements(By.XPATH, self.divs_dict['yandex']['xpath']['image_photo'])

                        scrollable_div = self.driver.find_elements(By.CLASS_NAME, self.divs_dict['yandex']['class_name']['scroll_container'])[0]
                        old_len = 0
                        self.scroll_down(scrollable_div, self.divs_dict['yandex']['xpath']['image_photo'], By.XPATH)
                        print(len(photos_divs))

                        if (len(photos_divs) > 0):
                            for photo in photos_divs:
                                photo_link = photo.get_attribute('src')
                                # print(photo_link)
                                mid_photos_building_pd.loc[len(mid_photos_building_pd)] = [
                                    address, place_name, found_address, coords, cur_url, photo_link, 'None', 0]
                            self.photos_building_pd = pd.concat([self.photos_building_pd, mid_photos_building_pd], ignore_index=True)
    
                orgs_inside_button = self.driver.find_elements(By.XPATH, self.divs_dict['yandex']['xpath']['organizations_inside'])
                if(len(orgs_inside_button) > 0):
                    #print(orgs_inside_button[0].get_attribute('innerHTML'))
                    orgs_inside_button[0].click()
                    time.sleep(1+round(random.uniform(0, 1), 2))
                    scrollable_div = self.driver.find_elements(
                        By.XPATH, self.divs_dict['yandex']['xpath']['organization_scrollable'])
                    if (len(scrollable_div) > 0):
                        self.scroll_down(
                            scrollable_div, self.divs_dict['yandex']['xpath']['organization_list_element_div'], By.XPATH)
                        org_lists = self.driver.find_elements(By.CLASS_NAME, self.divs_dict['yandex']['class_name']['organization_list'])
                        if(len(org_lists)>0):
                            org_divs = org_lists[0].find_elements(By.TAG_NAME, 'li')
        
                            for org_div in org_divs:
                                org_name = org_div.find_elements(
                                    By.XPATH, '.'+self.divs_dict['yandex']['xpath']['organization_name'])[0].get_attribute('textContent')
                                org_link = org_div.find_elements(
                                    By.XPATH, '.'+self.divs_dict['yandex']['xpath']['organization_link'])[0].get_attribute('href')
                                mid_organizations_pd.loc[len(mid_organizations_pd)] = [address, place_name, found_address, coords, org_name, org_link, 'None', 1, cur_url]
                            #                                           columns=['original_address','found_place','found_address','coords','place_name','place_link','ya_id','inside_original', 'original_link']
                            # print(len(org_name))

        self.save_organization_pd = pd.concat([self.save_organization_pd, mid_organizations_pd], ignore_index=True)
        return 1

    # saves all files===========================================================

    def save_all(self, filename):
        if (len(self.photos_organization_pd) > 0):
            self.save_photo_pd(filename)
        if (len(self.save_organization_pd) > 0):
            self.save_org_pd(filename)
        if (len(self.photos_building_pd) > 0):
            self.save_building_photos_pd(filename)

    # downloads photos by links==================================================

    # download images

    def move_panorama(self, panorama_div, angle, distance):
        # angle=270, dist<0 = move left
        angle_rad = math.radians(angle)
        start = panorama_div.location
        finish = {'x': int(start['x']+distance*math.sin(angle_rad)),
                  'y': int(start['y']+distance*math.cos(angle_rad))}
        ActionChains(self.driver).drag_and_drop_by_offset(
            panorama_div, finish['x'] - start['x'], finish['y'] - start['y']).perform()

    def panorama_tests(self):
        URL = 'https://yandex.ru/maps/63/irkutsk/house/ulitsa_lermontova_134/ZUkCaAVnTUwFVUJvYWJzdXxjbQA=/?ll=104.275114%2C52.240613&panorama%5Bdirection%5D=296.765776%2C12.750585&panorama%5Bfull%5D=true&panorama%5Bpoint%5D=104.272766%2C52.242252&panorama%5Bspan%5D=103.032442%2C60.000000&z=18'
        self.driver.get(URL)
        time.sleep(10)
        # marker_on_map=self.driver.find_elements(By.XPATH, "//ymaps[@class='ymaps3x0--marker']")
        actions = ActionChains(self.driver)
        len_check = 0
        while (len_check < 1):
            panorama_block = self.driver.find_elements(
                By.XPATH, self.divs_dict['yandex']['xpath']['panorama_main'])
            len_check = len(panorama_block)
        panorama_block = panorama_block[0]
        panorama_div = panorama_block.find_element(By.TAG_NAME, 'canvas')
        panorama_placemarks = panorama_block.find_elements(
            By.CLASS_NAME, self.divs_dict['yandex']['class_name']['panorama_placemark'])
        # panorama_controls=panorama_block.find_elements(By.CLASS_NAME, self.divs_dict['yandex']['class_name']['panorama_controls_all'])
        panorama_zoom = panorama_block.find_elements(
            By.CLASS_NAME, self.divs_dict['yandex']['class_name']['panorama_zoom'])
        # panorama_name=panorama_block.find_elements(By.CLASS_NAME, self.divs_dict['yandex']['class_name']['anorama_controls_all'])
        panorama_top_part = panorama_block.find_elements(
            By.CLASS_NAME, self.divs_dict['yandex']['class_name']['panorama_top_part_all'])
        panorama_logo = panorama_block.find_elements(
            By.CLASS_NAME, self.divs_dict['yandex']['class_name']['panorama_logo'])
        for element in panorama_top_part:
            self.driver.execute_script(
                "arguments[0].style.display = 'none';", element)
            time.sleep(0.5)
        for element in panorama_placemarks:
            self.driver.execute_script(
                "arguments[0].style.display = 'none';", element)
            time.sleep(0.5)
        for element in panorama_zoom:
            self.driver.execute_script(
                "arguments[0].style.display = 'none';", element)
            time.sleep(0.5)
        for element in panorama_logo:
            self.driver.execute_script(
                "arguments[0].style.display = 'none';", element)
            time.sleep(0.5)
        self.move_panorama(panorama_div, 270, -550)
        time.sleep(2)
        # self.move_panorama(panorama_div, 115, 500)

        # actions.move_to_element(panorama_div).perform()
        # actions.context_click(panorama_div).perform()

        time.sleep(0.5)
        # context_menu_option = self.driver.find_element(By.ID, 'context-menu-option')
        # context_menu_option.click()

        for i in range(20):

            filename = 'img_'
            cur_url = self.driver.current_url
            bPointCoords = re.findall(r'point%5D=\d+\.\d+%2C\d+\.\d+', cur_url)
            if (len(bPointCoords) > 0):
                filename = filename+bPointCoords[0]
            bDirCoords = re.findall(
                r'direction%5D=\d+\.\d+%2C\d+\.\d+', cur_url)
            if (len(bDirCoords) > 0):
                filename = filename+bDirCoords[0]
            filename = filename+'.png'

            actions.send_keys(Keys.DOWN).perform()
            time.sleep(2+round(random.uniform(0, 1), 2))
            panorama_placemarks = panorama_block.find_elements(
                By.CLASS_NAME, self.divs_dict['yandex']['class_name']['panorama_placemark'])
            for element in panorama_placemarks:
                self.driver.execute_script(
                    "arguments[0].style.display = 'none';", element)
                time.sleep(0.5)

            canvas_screenshot = panorama_div.screenshot_as_png
            with open(filename, "wb") as f:
                f.write(canvas_screenshot)

        time.sleep(0.5)
        

    def download_panorama(self, URL, filename):
        self.driver.get(URL)
        time.sleep(3+round(random.uniform(0, 1), 2))
        # marker_on_map=self.driver.find_elements(By.XPATH, "//ymaps[@class='ymaps3x0--marker']")
        if('panorama' not in self.driver.current_url):
            return
        actions = ActionChains(self.driver)
        len_check = 0
        while (len_check < 1):
            panorama_block = self.driver.find_elements(
                By.XPATH, self.divs_dict['yandex']['xpath']['panorama_main'])
            len_check = len(panorama_block)
            time.sleep(0.1)
        time.sleep(1+round(random.uniform(0, 1), 2))
        panorama_block = panorama_block[0]
        panorama_div = panorama_block.find_element(By.TAG_NAME, 'canvas')
        panorama_placemarks = panorama_block.find_elements(By.CLASS_NAME, self.divs_dict['yandex']['class_name']['panorama_placemark'])
        # panorama_controls=panorama_block.find_elements(By.CLASS_NAME, self.divs_dict['yandex']['class_name']['panorama_controls_all'])
        panorama_zoom = panorama_block.find_elements(By.CLASS_NAME, self.divs_dict['yandex']['class_name']['panorama_zoom'])
        # panorama_name=panorama_block.find_elements(By.CLASS_NAME, self.divs_dict['yandex']['class_name']['anorama_controls_all'])
        panorama_top_part = panorama_block.find_elements(By.CLASS_NAME, self.divs_dict['yandex']['class_name']['panorama_top_part_all'])
        panorama_logo = panorama_block.find_elements(By.CLASS_NAME, self.divs_dict['yandex']['class_name']['panorama_logo'])
        for element in panorama_top_part:
            self.driver.execute_script(
                "arguments[0].style.display = 'none';", element)
            #time.sleep(0.5)
        for element in panorama_placemarks:
            self.driver.execute_script(
                "arguments[0].style.display = 'none';", element)
            #time.sleep(0.5)
        for element in panorama_zoom:
            self.driver.execute_script(
                "arguments[0].style.display = 'none';", element)
            #time.sleep(0.5)
        for element in panorama_logo:
            self.driver.execute_script(
                "arguments[0].style.display = 'none';", element)
            #time.sleep(0.5)
        #self.move_panorama(panorama_div, 270, -550)
        #time.sleep(2)
        
        canvas_screenshot = panorama_div.screenshot_as_png
        with open(filename, "wb") as f:
            f.write(canvas_screenshot)
        # self.move_panorama(panorama_div, 115, 500)

        # actions.move_to_element(panorama_div).perform()
        # actions.context_click(panorama_div).perform()

        time.sleep(0.5)
        # context_menu_option = self.driver.find_element(By.ID, 'context-menu-option')
        # context_menu_option.click()

    

    # closes_driver==============================================================
    def close(self):
        self.driver.close()


# class ends====================================================================

def download_photos(filename):
    cur_pd = pd.read_csv(filename)
    print(cur_pd.columns)
    print(len(cur_pd))
    # cur_pd=cur_pd.loc[cur_pd['is_panorama']==0]
    print(cur_pd['is_panorama'])
    # save_file=pd.DataFrame(columns=['original_address','found_address','place_name','place_link','coords','photo_link','inside','ya_id', 'supposed_number_of_photos'])
    filepaths = []
    status_codes = []
    counter = 0
    filepaths = []
    status_codes = []
    for i, row in cur_pd.iterrows():
        counter += 1
        print(f'{counter}/{len(cur_pd)}')
        if ('panorama' in row['photo_link']):
            continue
        if (i > 10000000):
            break
        dirpath = 'photos'+'/'+'yandex/' + \
            str(row['lon'])+'_'+str(row['lat'])+'/'+str(row['place_name'])+'/'
        image_name = re.sub(r'X*L$|M$', 'XXXL', row['photo_link'])+'.jpg'
        total_filepath = dirpath+image_name.replace('/', '_')

        response = requests.get(row['photo_link'], stream=True)
        filepaths.append(total_filepath)
        status_codes.append(status_codes)
        print(response)
        if (not (os.path.isdir(dirpath))):
            os.makedirs(dirpath)
        with open(total_filepath, 'wb') as out_file:
            shutil.copyfileobj(response.raw, out_file)
        del response
        time.sleep(0.5)

    cur_pd['filepath'] = filepaths
    cur_pd['status_code'] = status_codes


def make_file_for_total(filename):
    cur_pd = pd.read_csv(filename)
    filepaths = []
    for i, row in cur_pd.iterrows():
        if ('panorama' in row['photo_link']):
            filepaths.append('this is panorama it will be downloaded later')
        else:
            dirpath = 'photos'+'/'+'yandex/' + \
                str(row['lon'])+'_'+str(row['lat']) + \
                '/'+str(row['place_name'])+'/'
            image_name = re.sub(r'X*L$|M$', 'XXXL', row['photo_link'])+'.jpg'
            total_filepath = dirpath+image_name.replace('/', '_')
            filepaths.append(total_filepath)
    cur_pd['filepath'] = filepaths
    cur_pd.to_csv('yandex_total_photos_links.csv')


def test():
    URL = 'https://yandex.ru/maps/63/irkutsk/?ll=104.280608%2C52.289590&z=12'
    # GET=LINKS================================================================
    '''
    parser=Parser()
    streets_df=pd.read_csv('streets.csv')
    streets_df=streets_df.loc[streets_df['street']=='Лермонтова Улица']
    streets=streets_df['house_intervals'].values[2].split(',')
    half_address='Иркутск, улица Лермонтова, '
    for street in streets:
        address=half_address+street
        print(address)
        parser.search_places_by_address(URL, address)
    parser.save_all(half_address)
    '''
    # GET=PHOTOS===============================================================
    parser = Parser()
    half_address = 'Иркутск, улица Лермонтова, '
    links_pd = pd.read_csv('Иркутск, улица Лермонтова, _yandex_org_links1.csv')
    links_pd = links_pd.drop_duplicates(subset=['place_link'])
    print(len(links_pd))
    for i, row in links_pd.iterrows():
        parser.parse_photos(row['place_link'],
                            row['place_name'], row['original_address'])
    parser.save_all(half_address)
    # download_photos('yandex_photos_links5.csv')


def test1(street, valuer):
    time_start = time.time()
    URL = 'https://yandex.ru/maps/63/irkutsk/?ll=104.280608%2C52.289590&z=12'
    dicter = {'Лермонтова Улица': 'Иркутск, улица Лермонтова, ', 'Старо-Кузьмихинская Улица': 'Иркутск, Старо-Кузьмихинская улица, ',
              'Фаворского Улица': 'Иркутск, улица Фаворского, ', 'Улан-Баторская Улица': 'Иркутск, Улан-Баторская улица, '}
    # GET=LINKS================================================================

    parser = Parser()
    streets_df = pd.read_csv('streets.csv')
    streets_df = streets_df.loc[streets_df['street'] == street]
    streets = []
    for k in range(valuer[0], valuer[1]):
        streets += streets_df['house_intervals'].values[k].split(',')
    print(streets)

    half_address = dicter[street]
    counter = 0
    for i, street in enumerate(streets):
        counter += 1
        print(f'{counter}/{len(streets)}')
        print(half_address+street)
        if (counter < 0):
            continue
        # print(i)
            # try:
        parser.search_places_by_address(URL, half_address+street)
        # except:
        #    print('ERROR')
        #    break
        if (i % 10 == 0 and i > 0):
            parser.save_all(half_address+'_' +
                            str(valuer[0])+'-'+str(valuer[1])+'_')
    parser.save_all(half_address+'_'+str(valuer[0])+'-'+str(valuer[1])+'_')
    print(time.time()-time_start)
    parser.close()


def slice_df(original_pd, slice_num=10):
    slice_len = int(len(original_pd)/slice_num)
    cutten_pds = []
    for i in range(10):
        start = i*slice_len
        end = (i+1)*slice_len
        if (end > len(original_pd)):
            end = len(original_pd)
        cutten_pds.append(original_pd[start:end])
    return cutten_pds


def convert_parse_photos(df, index):
    parser = Parser()
    for i, row in df.iterrows():
        parser.parse_photos(row['place_link'], row['original_address'])


def parse_all_organizations():
    addresses_df = pd.read_csv('organization_files/total_organizations.csv')
    sliced_addresses = slice_df(addresses_df, 10)
    threads = [
        Thread(target=convert_parse_photos, args=(sliced_addresses[i], i,))
        for i in range(10)
    ]

    for thread in threads:
        thread.start()

    for thread in threads:
        thread.join()


def test2():
    parser = Parser()
    parser.panorama_tests()


    

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
        stat=parser.parse_photos(row)
        try:
            #stat=parser.parse_photos(row)
            #(self, URL, found_place, coords, org_name, address, dgis_id):
            #parser.parse_photos(URL, found_place, coords, org_name, address, dgis_id)
            print(f'thread {index}: stat is {stat} ({row["found_place"]})')
            sliced_df.at[i, 'ya_stat']=stat
        except Exception as exc_text:
            parser.save_all(f'Иркутск_rows_{str(diap[0])}-{str(diap[1])}_thread_{str(index)}_orgphotos')
            print(f'Error: thread {index}: {counter}/{len(sliced_df)}| message: {exc_text}')
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
    streets_pd = pd.read_csv('addresses_stats_yandex_orgs.csv')
    streets_pd=streets_pd[streets_pd['ya_stat']<0]
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


def combine_files(filepath, subset_duplicate):
    total_filepath = os.path.abspath(os.curdir)+'/'+filepath
    filenames = os.listdir(total_filepath)
    #print(filenames)
    result = None
    for i, filename in enumerate(filenames):
        if (filename == 'total.csv'):
            continue
        if (i == 0):
            result = pd.read_csv(total_filepath+'/'+filename)
        else:
            result = pd.concat(
                [result, pd.read_csv(total_filepath+'/'+filename)])

    print(len(result))
    result=result[result['coords']!='-']
    if ('coords' in result.columns):
        lons = []
        lats = []
        for coord in result['coords']:
            splitted = coord.split(', ')
            if (len(splitted) == 1):
                splitted = coord.split(',')
            lon = float(splitted[0])
            lat = float(splitted[1])
            if (lon < lat):
                lon, lat = lat, lon
            lons.append(lon)
            lats.append(lat)
        result['lon'] = lons
        result['lat'] = lats

    if ('place_link' in result.columns):
        # result.loc[result['place_link']]
        # result=result[result['place_link']!='https://2gis.ru/irkutsk']
        # result=result[result["place_link"].str.contains("/branches/")==False]
        place_ids = result['place_link'].tolist()
        for i in range(len(place_ids)):
            place_ids[i] = remove_last(place_ids[i])
            place_ids[i] = get_id(place_ids[i])
        print(len(place_ids[i]))
        result['ya_id'] = place_ids
    if ('photo_link' in result.columns or 'image_url' in result.columns):
        if ('photo_link' in result.columns):
            pls = result['photo_link'].tolist()
        else:
            pls = result['image_url'].tolist()
        for i in range(len(pls)):
            # print(pls[i])
            link_3 = "None"
            if (pls[i][0:6] == 'https:'):
                #print('already parsed')
                continue
            link_1 = re.findall(r'url\([^\)\,]+\)', pls[i])
            if (len(link_1) > 0):
                link_2 = re.findall(r'\".+\"', link_1[0])
                link_3 = re.sub(r'\_\d+x\d+', '', link_2[0])
                link_3 = re.sub(r'\"', '', link_3)
            pls[i] = link_3
        result['photo_link'] = pls
    counter = 0
    if ('place_name' in result.columns and 'found_place' in result.columns):
        place_names = result['place_name'].tolist()
        found_names = result['found_place'].tolist()
        for i in range(len(result)):
            if (str(place_names[i]) == 'nan' and str(found_names[i]) != 'nan'):
                place_names[i] = found_names[i]
            if (str(place_names[i]) != 'nan' and str(found_names[i]) == 'nan'):
                found_names[i] = place_names[i]
            if (str(place_names[i]) == 'nan' and str(found_names[i]) == 'nan'):
                counter += 1
        result['place_name'] = place_names
        result['found_place'] = found_names

    # \xa0
    print(len(result))
    result = result.drop_duplicates(subset=subset_duplicate)
    result.to_csv(total_filepath+'/total.csv', index=False)


         

def parse_orgs_by_address(sliced_df, diap, index):
    parser=Parser()
    URL=parser.base_URL
    counter1=0
    parse_continue=True
    parsed_counter=0
    counter=0
    for i, row in sliced_df.iterrows():
        print(f'{index}: {counter}/{len(sliced_df)}')
        counter+=1
        
        stat=parser.search_places_by_address(URL, row['address'].replace(', empty', ''), True)
        if(parse_continue==False):
            break
        if(counter%10==0):
            parser.save_all(f'Иркутск_rows_{str(diap[0])}-{str(diap[1])}_thread_{str(index)}')
            sliced_df.to_csv(f'stats_{str(diap[0])}-{str(diap[1])}_thread_{str(index)}.csv')
        try:
            #stat=parser.search_places_by_address(URL, row['address'].replace(', empty', ''), True)
            print(f'thread {index}: stat is {stat} ({row["address"]})')
            sliced_df.at[i, 'ya_stat']=stat
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
                    time.sleep(5)
                    parser.recreate_driver()
                
                #print(f'Иркутск, {street}, {house_name} finish')
    parser.save_all(f'Иркутск_rows_{str(diap[0])}-{str(diap[1])}_thread_{str(index)}')
    parser.close()
    sliced_df.to_csv(f'stats_{str(diap[0])}-{str(diap[1])}_thread_{str(index)}.csv')


def parse_organizations_threads(diap, threads_num):
    print('start')
    time_start=time.time()
    streets_pd = pd.read_csv('addresses_stats.csv')
    streets_pd=streets_pd.loc[streets_pd['ya_stat']==-4]
    
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
    
    
    
def rebuild_stat_file(org_photos=False):
    if(org_photos==False):
        base_stat=pd.read_csv('addresses_stats.csv')
        output_name='addresses_stats_test.csv'
        id_name='id'
    else:
        base_stat=pd.read_csv('addresses_stats_yandex_orgs.csv')
        output_name='addresses_stats_yandex_orgs.csv'
        id_name='place_link'
    print(base_stat.columns)
    filenames=os.listdir(os.path.abspath(os.curdir))
    for filename in filenames:
        if(filename[0]!='.' and 'stat' in filename and '.csv' in filename and filename not in ['addresses_stats.csv', 'addresses_stats_ya_orgs.csv','addresses_stats_yandex_orgs.csv', 'addresses_stats_2gis_orgs.csv']):
            print(filename)
            add_pd=pd.read_csv(filename)
            print(add_pd.columns)
            ids=add_pd[id_name].tolist()
            stats=add_pd['ya_stat'].tolist()
            for i in range(len(add_pd)):
                if(stats[i]!=0):
                    base_stat.loc[base_stat[id_name]==ids[i], ['ya_stat']]=stats[i]
                    
    base_stat.to_csv(output_name, index=False)


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
            

def make_stat_org_file():
    main_pd=pd.read_csv('gathered_data/yandex/organizations/total.csv', index_col=False)
    main_pd=main_pd[main_pd['inside_original']==1]
    print(len(main_pd))
    main_pd['ya_stat']=[0 for i in range(len(main_pd))]
    main_pd.to_csv('addresses_stats_yandex_orgs.csv', index=False)
    
    

def download_photos_one_thread(filename, diap, index):
    print(f'{index}: thread_started')
    cur_pd=pd.read_csv(filename)
    cur_pd=cur_pd[diap[0]:diap[1]]
    #cur_pd=cur_pd.sample(n=10)
    #result_pd=pd.DataFrame(columns=['org_id', 'lon', 'lat', 'filename', 'filepath', 'exemplar'])
    k=-1
    for i, row in cur_pd.iterrows():
        k+=1
        if(k%1000==0):
            print(f'{index}: {k}/{len(cur_pd)}')
        if(os.path.isfile(row['filepath_found']) or 'api-version=2.0' in row['filepath_found']):
            #print('downloaded')
            continue
        if(row['is_panorama']==0):
            total_filepath=row['filepath_found']
            filepath='/'.join(total_filepath.split('/')[:-1])
            
            #print(f'{filepath}/{photo_name}')
            
            response = requests.get(row['photo_link'], stream=True)
            #print(response)
            if(response.status_code==200):
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
         

      
def download_panoramas_one_thread(filename, diap, index):
    parser=Parser()
    print(f'{index}: thread_started; range = {diap}')
    cur_pd=pd.read_csv(filename)
    cur_pd=cur_pd[cur_pd['is_panorama']==1]
    cur_pd=cur_pd[diap[0]:diap[1]]
    #cur_pd=cur_pd.sample(n=10)
    #result_pd=pd.DataFrame(columns=['org_id', 'lon', 'lat', 'filename', 'filepath', 'exemplar'])
    k=-1
    parsed=0
    for i, row in cur_pd.iterrows():
        k+=1
        
        print(f'{k}/{len(cur_pd)}')
        if(row['is_panorama']==1):
            #print(f'{index}: {k}/{len(cur_pd)}')
            photo_name=row['photo_link'].replace('/','_').replace('?','!')
            filepath=row['filepath']
            total_filepath=f'{filepath}/{photo_name}'
            if(not (os.path.isfile(total_filepath))):
                if (not (os.path.isdir(filepath))):
                    os.makedirs(filepath)
                parser.download_panorama(row['photo_link'], total_filepath)
            else:
                #print(f'{index}: {k}/{len(cur_pd)} already parsed')
                pass

                
            #print(f'{filepath}/{photo_name}')           
            #parser.download_panorama(row['photo_link'], total_filepath)
            #print(total_filepath)

         
            
def download_panoramas_threads(filename, start, threads_num):
     cur_pd=pd.read_csv(filename)
     cur_pd=cur_pd[cur_pd['is_panorama']==1]
     len_cur_pd=len(cur_pd)
     diaps=[]
     slicer=int((len_cur_pd-start)/threads_num)
     for i in range(threads_num):
         start_diap=start+i*slicer
         end_diap=start+(i+1)*slicer
         if(i==threads_num-1 and end_diap!=len_cur_pd-1):
             end_len=len_cur_pd-1
         diaps.append([start_diap, end_diap])
     
     threads = [
             Thread(target=download_panoramas_one_thread, args=(filename, diaps[i] ,i ,))
             for i in range(len(diaps))
         ]
                     
     for thread in threads:
         thread.start()
     
     for thread in threads:
          thread.join() 
          

          
          
def build_photos_links():
    org_photos=pd.read_csv('gathered_data/yandex/results/total_organizations.csv')
    found_addresses=org_photos['found_address'].tolist()
    original_addresses=org_photos['original_address'].tolist()
    photo_links=org_photos['photo_link'].tolist()
    coords=org_photos['photo_link'].tolist()
    filepaths=[]
    filepaths1=[]
    filepaths2=[]
    lons=[]
    lats=[]
    
    for i, row in org_photos.iterrows():
        found_address=re.sub(r'\, \d\d\d\d\d\d', '', row['found_address']).replace(', Иркутск', '')
        photo_name=row['photo_link'].replace('/','_').replace('?','!')
        lat=re.findall(r'\d+\.\d+', row['coords'])[0]
        lon=re.findall(r'\d+\.\d+', row['coords'])[1]
        if(float(lon)<float(lat)):
            lon, lat=lat, lon
        filepaths.append(f"photos/yandex/Иркутск, {found_address.replace('/','_').replace('?','!')}/{photo_name}")
        filepaths1.append(f"photos_original/Иркутск, {row['original_address'].replace('/','_').replace('?','!')}/{photo_name}")
        filepaths2.append(f"photos_coords_upd/yandex/{lon}_{lat}/{photo_name}")
        lons.append(lon)
        lats.append(lat)
    org_photos['filepath_found']=filepaths
    org_photos['filepath_original']=filepaths1
    org_photos['filepath_coords']=filepaths2
    org_photos['is_panorama']=[0 for i in range(len(org_photos))]
    org_photos['longitude']=lons
    org_photos['latitude']=lats
    org_photos=org_photos.filter(['filepath_original', 'filepath_found', 'filepath_coords', 'original_address', 'found_address', 'found_name', 'photo_link','ya_id', 'longitude', 'latitude', 'is_panorama'])
    #org_photos.to_csv('gathered_data/yandex/results/all_photo_test.csv', index=False)
    
    build_photos=pd.read_csv('gathered_data/yandex/results/total_building.csv')
    found_addresses=build_photos['found_address'].tolist()
    original_addresses=build_photos['original_address'].tolist()
    filepaths=[]
    filepaths1=[]
    filepaths2=[]
    lons=[]
    lats=[]
    for i, row in build_photos.iterrows():
        found_address=re.sub(r'\, \d\d\d\d\d\d', '', row['found_address']).replace(', Иркутск', '')
        photo_name=row['photo_link'].replace('/','_').replace('?','!')
        lat=re.findall(r'\d+\.\d+', row['coords'])[0]
        lon=re.findall(r'\d+\.\d+', row['coords'])[1]
        if(float(lon)<float(lat)):
            lon, lat=lat, lon
        filepaths.append(f"photos/yandex/Иркутск, {found_address.replace('/','_').replace('?','!')}/{photo_name}")
        filepaths1.append(f"photos_original/Иркутск, {row['original_address'].replace('/','_').replace('?','!')}/{photo_name}")
        filepaths2.append(f"photos_coords_upd/yandex/{lon}_{lat}/{photo_name}")
        lons.append(lon)
        lats.append(lat)
    build_photos['filepath_found']=filepaths
    build_photos['filepath_original']=filepaths1
    build_photos['filepath_coords']=filepaths2
    build_photos['longitude']=lons
    build_photos['latitude']=lats
    build_photos=build_photos.filter(['filepath_original', 'filepath_found', 'filepath_coords', 'original_address', 'found_address', 'found_name', 'photo_link','ya_id', 'longitude', 'latitude' ,'is_panorama'])
    #build_photos.to_csv('gathered_data/yandex/results/all_photo_test_2.csv', index=False)
    
    
    #res_pd=pd.read_csv('gathered_data/yandex/results/all_photo_test.csv')
    res_pd=pd.concat([org_photos, build_photos])
    res_pd.to_csv('gathered_data/yandex/results/all_photos_all.csv', index=False)

     


if __name__ == '__main__':
    #parse_photos_orgs_threads([0, 5], 1)
    #parse_organizations_threads([0,60], 1)
    #clear_saved_files('Иркутск_rows_0-320')
    #rebuild_stat_file(org_photos=True)
    #download_photos_threads('gathered_data/yandex/results/all_photos_all.csv', 0, 1)
    #download_photos_one_thread('gathered_data/yandex/results/all_photos_all.csv', [0, 1000000], 0)
    #download_panoramas_threads('gathered_data/yandex/results/all_photos_all.csv', 0, 1)
    build_photos_links()
    
    
    #stats=pd.read_csv('addresses_stats.csv')
    #print(len(stats[stats['ya_stat']==0]))
    
    #stats=pd.read_csv('addresses_stats_yandex_orgs.csv')
    #print(len(stats[stats['ya_stat']==-1]))
    
    
    #parser=Parser()
    #print(parser.search_places_by_address(parser.base_URL, 'Иркутск, Декабристов Площадь'))
    #print(parser.search_places_by_address(parser.base_URL, 'Иркутск, Бородина Улица, стр39/1'))
    #parser.save_all('ya_test')
    #combine_files('gathered_data/yandex/organizations', ['place_link'])
    #combine_files('gathered_data/yandex/building_photos', ['photo_link'])
    #combine_files('gathered_data/yandex/organization_photos', ['photo_link'])
    #combine_files('gathered_data/yandex/results', ['photo_link'])
    #make_stat_org_file()
    
    
    
    
    
