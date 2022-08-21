# Plan de actiune,
 In azure blob storage sub folderele 14 nu trebuie sa te uiti ca e device id  nivelul bateriei, nu sunt date telemetrice
 Sub folderul 18 is datele de la telefonul lui Edi, sari peste, template-ul nu e la fel ca la telefonul meu, atentie pe viitor
 In folderul 29 is datele de la telefonul meu sub forma de an --> luna --> zi --> ora --> minut

 # Parsare json
 prima data sa parsed si sa iau device id, timestampul si telemetry data

 dupa sa organizez aceste date dupa ora/ minut bazat pe numele fisierului sau se pot grupa ulterior dupa timestamp
 sa ii dai luna, ziua, si intre ce ore sa ia datele si sa faca un singur dataframe
# MASTER FOLDER MAPPING
Inside Azure Container "disertatie" there is another folder named "fb1231ec-5b6b-476b-9751-71ae87ed1766"
inside this folder azure creates a new folder for each device, in folder "14" it recordes battery level, 
which in not needed for this thesis, in the other folders there is data for a device organised by time.

Lavi's folder --> 29 --> Device's Id\
Edi's folder  --> 18 --> Device's Id\
Paul's folder --> 19 --> Device's Id\
Mara's folder --> 4 --> Device's Id\    
Darius's folder --> 5 --> Device's Id
# DATA COLLECTION:
 ## STEPPER (Scari Urcare):

#### **01.06.2022**
    -> Edi: 
            19.03 -> 19.05
            19.17 -> 19.20
    -> Lavi: 
            19.06 -> 19.16
            19.22 -> 19.28
#### **15.06.2022**
(Datele din ziua asta au o eroare de +-1/2 minute 
fata de momentul initial si/sau final)
    -> Edi:

    -> Paul: subfolderul 16
            20:50 -> 20:55
            21:07 -> 21:11
    -> Mara: subfolderul 16
            20:30 -> 20:41
    -> Darius: subfolderul 16
              20:22 -> 20:28
              20:42 -> 20:47
    -> Lavi: 
 ## LAYING
 (Datele din ziua asta au o eroare de +-1/2 minute 
fata de momentul initial si/sau final)
#### **14.07.2022**
    -> Paul: subfolderul 16
    

 ## SITTING:
(Datele din ziua asta au o eroare de +-1/2 minute 
fata de momentul initial si/sau final)
 #### **15.06.2022**
    -> Paul: subfolderul 16
           19:01 -> 19:16
    
    -> Mara: subfolderul 16
           19:03 -> 19.16
 #### **03.08.2022**
    -> Darius: subfolderul 16
             17:48 -> 17:58
            

 ## WALKING
(Datele din ziua asta au o eroare de +-1/2 minute 
fata de momentul initial si/sau final)
 #### **15.06.2022**

    -> Paul: subfolderul 16
            19:57 -> 20:09
    -> Mara: subfolderul 16
            19:57 -> 20:09
    -> Darius: subfolderul 16
            19:57 -> 20:09
    
 ## STANDING
(Datele din ziua asta au o eroare de +-1/2 minute 
fata de momentul initial si/sau final)
 #### **15.06.2022**

    -> Paul: subfolderul 16
              19:36 -> 19:48
    -> Mara: subfolderul 16
              19:36 -> 19:48
    -> Darius: subfolderul 16
              19:36 -> 19:48


_________________
o functie sa ia luna, ziua, ora si minutele intre care s-a petrecut tot si sa faca merge fara sa sorteze nimic, doar da uneasca toate fisierele si sa faca un folder numit merged si sa scrie fisierul acolo

o functie sa ia fisierul merged si sa selecteze informatia sa o puna intr-un dataframe --> exista deja 

_______________________________________________
 accelerometru :
 x  = 1
 y =2 
 z =3
__________________________________________________________
in loc de functia care sa faca merge la fisere si sa scrie un fisier nou, 
sa chemi functia care face df pentru fiecare fisier si sa le unesti dupa 
intr un singur dataframe

o alta functie sa imparta datele telemetrice in coloane 