import os
#from cv2 import cv2
from PIL import Image
import pandas as pd
import shutil
import re



def delete_repeats():
    total_photos_count=0
    addresses_ya=os.listdir('photos/yandex')
    delete_list=[]
    k=0
    for direc in addresses_ya:
        k+=1
        print(f"{direc}, {k}/{len(addresses_ya)}")
        photos=os.listdir(f"photos/yandex/{direc}")
        #total_photos_count+=len(photos)
        for photo in photos:
            try:
                im = Image.open(f"photos/yandex/{direc}/{photo}")
            except:
                continue
            for photo1 in photos:
                if(photo!=photo1):
                    try:
                        im1=Image.open(f"photos/yandex/{direc}/{photo1}")
                        if(im==im1):
                            os.remove(f"photos/yandex/{direc}/{photo1}")
                    except:
                        continue
                        
                        
        photos=os.listdir(f"photos/yandex/{direc}")
        total_photos_count+=len(photos)
                  


    print(total_photos_count)
    

def get_image_format(image_path):
    try:
        with Image.open(image_path) as img:
            return img.format
    except IOError:
        print(f"Error: Could not open or identify image at {image_path}")
        return None
    except SyntaxError:
        print(f"Error: Invalid image file at {image_path}")
        return None
    


def count_photos():
    ya_count=0
    dg_count=0
    addresses_ya=os.listdir('photos_coords/yandex')
    addresses_dg=os.listdir('photos_coords/2gis')
    for direc in addresses_ya:
        photos=os.listdir(f"photos_coords/yandex/{direc}")
        ya_count+=len(photos)
        
    for direc in addresses_dg:
        photos=os.listdir(f"photos_coords/2gis/{direc}")
        dg_count+=len(photos)
        
    print(ya_count)
    print(dg_count)
    print(ya_count+dg_count)
    print(dg_count/len(addresses_dg))
    print(ya_count/len(addresses_ya))
    

def remake_dataset():
    log_file_dg=pd.read_csv('gathered_data/2gis/results/all_photos_all.csv')
    log_file_ya=pd.read_csv('gathered_data/yandex/results/all_photos_all.csv')
    #print(log_file_dg.columns)
    counter=0
    
    for i, row in log_file_dg.iterrows():
        
        total_filepath=row['filepath_found']
        if(not(os.path.isfile(total_filepath))): 
            if('api-version' not in total_filepath):
                print(total_filepath)
        else:
            
            '''
            filepath1='/'.join(row['filepath_original'].split('/')[:-1])
            if(not(os.path.isdir(filepath1))):
                os.makedirs(filepath1)
            shutil.copyfile(total_filepath, row['filepath_original'])
            '''
            if('.png' not in row['filepath_coords'] and '.jpg' not in row['filepath_coords'] and '.jpeg' not in row['filepath_coords']):
                row['filepath_coords']=row['filepath_coords']+'.'+get_image_format(f"{total_filepath}").lower()
                
            filepath2='/'.join(row['filepath_coords'].split('/')[:-1])
            if(not(os.path.isdir(filepath2))):
                os.makedirs(filepath2)
            shutil.copyfile(total_filepath, row['filepath_coords'])
                
        #break
    
    
    for i, row in log_file_ya.iterrows():
        photo_name=row['photo_link'].replace('/','_').replace('?','!')
        total_filepath=row['filepath_found']
        if(not(os.path.isfile(total_filepath))):    
            #print(total_filepath)
            counter+=1
        else:
            #continue
            filepath1='/'.join(row['filepath_coords'].split('/')[:-1])
            if('.png' not in row['filepath_coords'] and '.jpg' not in row['filepath_coords'] and '.jpeg' not in row['filepath_coords']):
                row['filepath_coords']=row['filepath_coords']+'.'+get_image_format(f"{row['filepath_found']}").lower()

            if(not(os.path.isdir(filepath1))):
                os.makedirs(filepath1)
            shutil.copyfile(total_filepath, row['filepath_coords'])
    print(counter)
    print(len(log_file_ya)-counter)
    print(len(log_file_ya))
    
    
if __name__=='__main__':
    #delete_repeats()
    #count_photos()
    remake_dataset()    
    