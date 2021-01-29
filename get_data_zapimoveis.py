"""
This script  if for gathering data and formating features from terrain in Pernambuco, using the zapimoveis_scraper.

Zapimoveis is a brasilian portal that gather offerts of properties. 

"""

import pandas as pd
import zapimoveis_scraper as zap



def get_data(regioes):
    """ 
Obtain data and features from the properties in Pernambuco in the cities given by the parameter.

Parameters:
   list
   List with the name of cities of the pernambuco state.
Returns:
   DataFrame 
   Dataframe with the avaiable data from properties.
   """
    preco = []
    area = []
    descricao = []
    add = []
    for i in regioes:
        #Get the data for each city.
        print(i)
        data = zap.search(localization="pe+"+i, num_pages=8,acao='venda',tipo='terrenos-lotes-condominios')  
        #Get the price, land area and address.
        for j in range(len(data)): 
            #Price
            preco.append(int(data[j].price.split(' ')[1].replace('.',''))) 
            #Area
            if data[j].total_area_m2 == '':
                area.append('Nan')
            else:
                area.append(float (data[j].total_area_m2.split(' ')[0]))
            #Land description
            descricao.append(data[j].description)
            add.append(data[j].address)
            #for k in data[j].address.split(', '):
            #    add1.append(data[i].address.split(', ')[0])
            #    add2.append(data[i].address.split(', ')[1])
                
    
    colunas = {'preco':preco,'area':area,'add':add,
               'descricao':descricao}
    dados = pd.DataFrame(colunas) #DataFrame Creation
    return dados

def m2_ivv(df):
    """ 
 Get the IVV "índicie de velocidade de venda" in portugues or selling velocity index to each property.

Parameters:
   Dataframe
   Data from cities.
Returns:
   DataFrame 
   Data from properties with IVV.
   """
    
    CAMARAGIBE = ['Alberto Maia', 'Timbi']
    JABOATAOI = ['Barra de Jangada', 'Candeias', 'Piedade']
    JABOATAOII = ['Sucupira', 'Vargem Fria', 'Muribeca']
    OLINDA = ['Bairro Novo', 'Casa Caiada', 'Ouro Preto', 'Peixinhos' , 'Rio Doce']
    PAULISTA = ['Paulista', 'Janga', 'Maranguape I', 'Maranguape II', 'Pau Amarelo']
    RECIFE_CENTRO = ['Boa Vista', 'Santo Amaro', 'São José', 'Soledade']
    RECIFE_NORO = ['Aflitos', 'Apipucos', 'Casa Amarela', 'Casa Forte', 'Espinheiro', 'Graças', 'Jaqueira', 'Macaxeira', 'Parnamirim', 'Poço da Panela', 'Tamarineira']
    RECIFE_NORTE = ['Água Fria Beberibe', 'Campo Grande', 'Encruzilhada', 'Rosarinho', 'Torreão']
    RECIFE_OESTE = ['Caxangá', 'Cordeiro', 'Ilha do Leite', 'Ilha do Retiro', 'Madalena', 'Torre', 'Várzea']
    RECIFE_SUDOESTE = ['Curado', 'Tejipió']
    RECIFE_SUL = ['Boa Viagem', 'Imbiribeira' , 'Setúbal'];
    SAO_LOURENCO = ['São Lourenço', 'Muribara', 'Parque Capibaribe']
    df['preco m2'] = df['preco']
    df['Ivv'] = 0
    for i in range(len(df['Ivv'])):
        if df['bairro'][i] == 'Paiva':
            df['Ivv'][i] = 3.7      #Cabo de santo agostinho
        elif df['bairro'][i] == 'Garapu':
            df['Ivv'][i] = 2.6      #Cabo de santo agostinho 2
        elif df['bairro'][i]in CAMARAGIBE:
            df['Ivv'][i] = 20.0
        elif df['bairro'][i]in JABOATAOI:
            df['Ivv'][i] = 8.2
        elif df['bairro'][i]in JABOATAOII:
            df['Ivv'][i] = 1.9
        elif df['bairro'][i]in OLINDA:
            df['Ivv'][i] = 1.4
        elif df['bairro'][i]in PAULISTA:
            df['Ivv'][i] = 39.9
        elif df['bairro'][i]in RECIFE_CENTRO:
            df['Ivv'][i] = 2.4
        elif df['bairro'][i]in RECIFE_NORO:
            df['Ivv'][i] = 3.4
        elif df['bairro'][i]in RECIFE_NORTE:
            df['Ivv'][i] = 4.4
        elif df['bairro'][i]in RECIFE_OESTE:
            df['Ivv'][i] = 1.3
        elif df['bairro'][i]in RECIFE_SUDOESTE:
            df['Ivv'][i] = 46.2
        elif df['bairro'][i]in RECIFE_SUL:
            df['Ivv'][i] = 9.6
        elif df['bairro'][i]in SAO_LOURENCO:
            df['Ivv'][i] = 4.2
        
        if type(df['area'][i]) == int:
            df['preco m2'][i] = float(df['preco'][i])/df['area'][i]
        else:
            df['preco m2'][i] = 'nan'
        print(i)
    return df

def merge_data(df,df2):
    """
    Merge old and new data together, removing duplicates
    Parameters:
        Dataframe, Dataframe
        Old and new data
    Returns:
        DataFrame 
        Data from properties with IVV.
   """
    df_concat = pd.concat([df,df2])
    df_concat.drop(['cidade','Ivv','preco m2'],axis=1,inplace=True)
    df_concat_no_dupli = df_concat.drop_duplicates()
    df_concat_no_dupli = m2_ivv(df_concat_no_dupli)
    return df_concat_no_dupli


def geo_coding(df):
    from geopy.extra.rate_limiter import RateLimiter
    from geopy.geocoders import Nominatim
    locator = Nominatim(user_agent='myGeocoder')
    # 1 - conveneint function to delay between geocoding calls
    geocode = RateLimiter(locator.geocode, min_delay_seconds=1)
    # 2- - create location column
    df['location'] = df['add'].apply(geocode)
    # 3 - create longitude, laatitude and altitude from location column (returns tuple)
    df['point'] = df['location'].apply(lambda loc: tuple(loc.point) if loc else None)
    # 4 - split point column into latitude, longitude and altitude columns
    df[['latitude', 'longitude', 'altitude']] = pd.DataFrame(df['point'].tolist(), index=df.index)
    #geometry = [Point(xy) for xy in zip(df["longitude"],df["latitude"])]
    #crs = {'init':'epsg:4326'}
    #geo_df = gpd.GeoDataFrame(df,crs=crs, geometry= geometry)
    return df #, geo_df



cities = ['recife','cabo-de-santo-agostinho','goiana', \
                    'igarassu', 'ilha-de-itamaraca','ipojuca', 'itapissuma', \
                    'jaboatao-dos-guararapes','moreno', 'olinda', 'paulista','sao-lourenco-da-mata']
#'abreu-e-lima'
    
df = get_data(cities)
df = df.drop_duplicates() #Removes all duplicate data
df['preco_m2'] = df['preco'].divide(df['area'].astype(float)) 

df = geo_coding(df)
#df = m2_ivv(df) #IVV not updated.

"""
If is the first time using, leave the lines as commentary
"""
#old_df = pd.read_csv('data.csv')
#df = merge_data(df,old_df) #New dataframe wih old and new data
""""""
df.to_csv('data.csv') #Export the data to an csv file.
