# -*- coding: utf-8 -*-

import re
import requests
import numpy
import csv
import pymysql

people= 'https://swapi.dev/api/people/'
# i = 82
i = 1
k = 0
#out_data = [['','','','',''],['','','','','']]
out_data = [['name','homeworld.name','height','gender','starships.name']]
people_txt = ''
people_URL = people + str(i)
people_info = requests.get(people_URL)
while people_info.status_code == 200:
#while i <= 1:
    people_txt = people_info.text
    homeworld_last_index = people_txt.find('https://swapi.dev/api/planets/', 0, len(people_txt))
    z = people_txt.find('"', homeworld_last_index, len(people_txt))
    planet_URL = people_txt[people_txt.find('https://swapi.dev/api/planets/', 0, len(people_txt)):z]
    planet_info_txt = requests.get(planet_URL).text
    planet_name_first_index = planet_info_txt.find('name', 0, len(planet_info_txt)) + 7
    planet_name_last_index = planet_info_txt.find('"', planet_name_first_index, len(planet_info_txt) )
    planet_name = planet_info_txt[planet_name_first_index: planet_name_last_index]
    if planet_name == 'Tatooine':
        starships_info_first_index = people_txt.find('starships', 0, len(people_txt)) + 12
        starships_info_last_index = people_txt.find(']', starships_info_first_index, len(people_txt))
        https_count = people_txt.count('https' , starships_info_first_index, starships_info_last_index)
        https_index = [people_txt.start() for people_txt in re.finditer ('https', people_txt[starships_info_first_index: starships_info_last_index])]
        starships_data = people_txt[starships_info_first_index: starships_info_last_index]
        if https_count == 0:
            newrow = ['','','','','']
            out_data = numpy.vstack([out_data, newrow])  
            k += 1
            name_first_index = people_txt.find('name', 0, len(people_txt)) + 7
            name_last_index = people_txt.find('"', name_first_index, len(people_txt))
            name = people_txt[name_first_index: name_last_index]
            height_first_index = people_txt.find('height', 0, len(people_txt)) + 9
            height_last_index = people_txt.find('"', height_first_index, len(people_txt))              
            heigth = people_txt[height_first_index: height_last_index]
            gender_first_index = people_txt.find('gender', 0, len(people_txt)) + 9
            gender_last_index = people_txt.find('"', gender_first_index, len(people_txt))
            gender = people_txt[gender_first_index: gender_last_index]
            starship_name = 'no starship'
            out_data[k][0] = name
            out_data[k][1] = planet_name
            out_data[k][2] = heigth
            out_data[k][3] = gender
            out_data[k][4] = starship_name         
        else:
            for j in range(0, https_count):
                k += 1
                newrow = numpy.array (['','','','',''], dtype=(str, 16))
                out_data = numpy.vstack([out_data, newrow])   
                #out_data = numpy.array(out_data)
                name_first_index = people_txt.find('name', 0, len(people_txt)) + 7
                name_last_index = people_txt.find('"', name_first_index, len(people_txt))
                name = people_txt[name_first_index: name_last_index]
                height_first_index = people_txt.find('height', 0, len(people_txt)) + 9
                height_last_index = people_txt.find('"', height_first_index, len(people_txt))              
                heigth = people_txt[height_first_index: height_last_index]
                gender_first_index = people_txt.find('gender', 0, len(people_txt)) + 9
                gender_last_index = people_txt.find('"', gender_first_index, len(people_txt))
                gender = people_txt[gender_first_index: gender_last_index]
                starships_URL_last_index = starships_data.find('"', https_index[j], len(starships_data))
                starship_URL = starships_data[https_index[j]: starships_URL_last_index]
                starship_info_txt = requests.get(starship_URL).text
                starship_name_first_index = starship_info_txt.find('name', 0, len(starship_info_txt)) + 7
                starship_name_last_index = starship_info_txt.find('"', starship_name_first_index, len(starship_info_txt))
                starship_name = starship_info_txt[starship_name_first_index: starship_name_last_index]
                out_data[k][0] = numpy.array(name, dtype=(str, 16))
                out_data[k][1] = planet_name
                out_data[k][2] = heigth
                out_data[k][3] = gender
                out_data[k][4] = starship_name           
    i += 1
    people_URL = people + str(i)
    people_info = requests.get(people_URL)
       
      
print ('out_data =',out_data)

out_file_csv = open('out_data.csv', 'w')
with out_file_csv:
    writer = csv.writer(out_file_csv)
    writer.writerows(out_data)
    
conn = pymysql.connect(host='localhost', user='root', password='root', db='cpp_data', charset="utf8")
cursor = conn.cursor () 
with open('out_data.csv','r',encoding='utf-8') as csvfile:
    read=csv.reader(csvfile)
    for each in list(read)[1:]:
        i=tuple(each)       
        sql = "insert into cpp_data.sw values"+str(i)
        cursor.execute(sql)
    conn.commit()
    cursor.close()
    conn.close()
