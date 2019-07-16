mkdir data
cd data

curl https://download.cms.gov/nppes/NPPES_Data_Dissemination_July_2019.zip >> data.zip 
unzip data.zip
curl http://nucc.org/images/stories/CSV/nucc_taxonomy_191.csv >> nucc_taxonomy_191.csv
