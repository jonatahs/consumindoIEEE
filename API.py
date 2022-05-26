# realizando imports
import requests
import json
import pandas as pd
import pyodbc
# url com apikey
url = 'http://ieeexploreapi.ieee.org/api/v1/search/articles?apikey=SUA_CHAVE_AQUI&format=json&max_records=250&start_record=1&sort_order=asc&sort_field=article_number&article_title=big+data&publication_title=big+data'

# testando resposta da api
resposta = requests.get(url)

print(resposta)

dados_json = json.loads(resposta.text)

artigos_id = dados_json['articles']
print(type(artigos_id))

df = pd.DataFrame(artigos_id)

lin = 0
listaAutores = []
lista_id_Autores = []
lista_url_autores = []
lista_ordem_autor = []
for item in df['authors'].values:
  lin += 1
  lista_linha = []
  for i in item.values():
    for p in i:
      # Coletando informacoes no dicionario
      nomesIDS = p.get('full_name')
      IDS = p.get('id')
      urlAutor = p.get('authorUrl')
      ordemAutor = p.get('author_order')
      # add dados na lista vazia 
      listaAutores.append(nomesIDS)
      lista_id_Autores.append(IDS)
      lista_url_autores.append(urlAutor)
      lista_ordem_autor.append(ordemAutor)
# Convertendo listas para dataframe
colunaAutores = pd.DataFrame(listaAutores)
coluna_id_autores = pd.DataFrame(lista_id_Autores)
coluna_url_autores = pd.DataFrame(lista_url_autores)
coluna_ordem_autor = pd.DataFrame(lista_ordem_autor)
# dropando coluna irrelevantes para analise
df.drop(columns = ['authors','index_terms','isbn_formats','rank'], inplace = True)
# add colunas novas sobre autores
df['author_order'] = coluna_ordem_autor
df['id_author'] = coluna_id_autores
df['author'] = colunaAutores
df['url_author'] = coluna_url_autores




conn = pyodbc.connect('Driver={ODBC Driver 17 for SQL Server};'
                      'Server=localhost;'
                      'Database=MBA;'
                      'Trusted_Connection=yes;')
cursorSQL = conn.cursor()

for index, linha in df.iterrows():
    
    cursorSQL.execute("INSERT INTO extracaoAPI(doi,title,publisher,isbn,issn,access_type,content_type,abstract,article_number,pdf_url,html_url,abstract_url,publication_title,conference_location,conference_dates,publication_number,is_number,publication_year,publication_date,start_page,end_page,citing_paper_count,citing_patent_count,author_order,id_author,author,url_author) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",str(linha.doi),str(linha.title),str(linha.publisher),str(linha.isbn),str(linha.issn),str(linha.access_type),str(linha.content_type),str(linha.abstract),str(linha.article_number),str(linha.pdf_url),str(linha.html_url),str(linha.abstract_url),str(linha.publication_title),str(linha.conference_location),str(linha.conference_dates),str(linha.publication_number),str(linha.is_number),str(linha.publication_year),str(linha.publication_date),str(linha.start_page),str(linha.end_page),str(linha.citing_paper_count),str(linha.citing_patent_count),str(linha.author_order),str(linha.id_author),str(linha.author),str(linha.url_author))
cursorSQL.commit()
cursorSQL.close()

print('############## finalizado #############')
