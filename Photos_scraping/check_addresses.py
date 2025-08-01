import pandas as pd
from natasha import AddrExtractor, MorphVocab

'''
morph_vocab = MorphVocab()
extractor = AddrExtractor(morph_vocab)

def fix_addr(text):
    matches = extractor(text)
    return (', '.join(f'{match.fact.type or ""} {match.fact.value}' for match in matches)) or None


df = pd.DataFrame([[1,'Bob', ' москва'],
                  [2,'Sally', 'отдых'],
                  [3,'Scott', 'хабаровск'], 
                  [4,'Вася', 'село Верхний Низ, ул. Ленина, д. 17']],  
columns=['id','name', 'street'])
df['street1'] = df['street'].map(fix_addr)
'''

wrong_types=['СНТ','СПК', 'Некоммерческое СТ', 'некоммерческое СТ', 'Садоводческое товарищество', 'Садоводческий кооператив',
             'садоводческого кооператива','Садоводческое товарищество','ДНТ','Некоммерческое садоводческое товарищество','садоводческое некоммерческое товарищество',
             'Садоводческое некоммерческое товарищество','некоммерческое садоводческое товарищество', 'садоводческое товарищество']



streets_df=pd.read_csv('adress_FIAS.csv').dropna(subset=['address'])
streets_list=streets_df['location'].tolist()
house_list=streets_df['buildingnumber'].tolist()

old_streets_df=pd.read_csv('addresses_stats.csv')
old_streets=old_streets_df['address'].tolist()
#print(old_streets)

new_addrs=[]
new_addrs_no_dom=[]
new_addrs_no_str=[]
new_addrs_closer=[]

counter=0

for i in range(len(streets_list)):
    print(f"%{house_list[i]}%")
    new_addr=f"Иркутск, {streets_list[i]}, {house_list[i]}"
    new_addr_no_dom=f"Иркутск, {streets_list[i]}, {house_list[i].replace('д. ', '')}"
    new_addr_no_str=f"Иркутск, {streets_list[i]}, {house_list[i].replace('стр. ', '')}"
    new_addr_closer=f"Иркутск, {streets_list[i]}, {house_list[i].replace('стр. ', 'стр').replace(' к. ', 'к')}"
    wwrt=False
    for wrong_type in wrong_types:
       if(wrong_type in new_addr):
           wwrt=True
           break
    
    
    if(new_addr_no_dom not in old_streets and 
       new_addr_no_str not in old_streets and
       new_addr_closer not in old_streets and
       wwrt == False):
        print(new_addr)
        counter+=1
        
    new_addrs.append(new_addr)
    new_addrs_no_dom.append(new_addr_no_dom)
    new_addrs_no_str.append(new_addr_no_str)
    new_addrs_closer.append(new_addr_closer)
    

print(counter)


'''
counter=0
for i in range(len(old_streets)):
    wwrt=False
    for wrong_type in wrong_types:
       if(wrong_type in old_streets[i]):
           wwrt=True
    if(wwrt ==False and old_streets[i] not in new_addrs and 
       old_streets[i] not in new_addrs_no_dom and
       old_streets[i] not in new_addrs_no_str and
       old_streets[i] not in new_addrs_closer):
        print(old_streets[i])
        counter+=1
        
print(counter)
'''