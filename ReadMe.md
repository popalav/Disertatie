# Plan de actiune,
 In azure blob storage sub folderele 14 nu trebuie sa te uiti ca e device id  nivelul bateriei, nu sunt date telemetrice
 Sub folderul 18 is datele de la telefonul lui Edi, sari peste, template-ul nu e la fel ca la telefonul meu, atentie pe viitor
 In folderul 29 is datele de la telefonul meu sub forma de an --> luna --> zi --> ora --> minut

 # Parsare json
 prima data sa parsed si sa iau device id, timestampul si telemetry data

 dupa sa organizez aceste date dupa ora/ minut bazat pe numele fisierului sau se pot grupa ulterior dupa timestamp
 sa ii dai luna, ziua, si intre ce ore sa ia datele si sa faca un singur dataframe

 deviceId
 enqueuedTime
 telemetry