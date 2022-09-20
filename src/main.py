from functions.funciones import *
from sqlalchemy import create_engine
import sqlite3

def run():
    df_main = requerimiento_principal('http://api.tvmaze.com/schedule/web', 
                                  '2020-12-01', '2020-12-31').reset_index()
    
    # Procesamiento de los datos
    df_main = normalizar_diccionarios(df_main, '_embedded', 'id')
    df_main = normalizar_diccionarios(df_main, '_links', 'id')
    df_main = normalizar_diccionarios(df_main, 'image', 'id')
    df_main = normalizar_diccionarios(df_main, 'rating', 'id')

    # convertir los nan a None
    df_main.astype(object).where(pd.notnull(df_main),None)

    list_countries = []    
    for i in range(len(df_main)):
        if  (pd.isna(df_main['show.webChannel.country.name'][i])) == True:
            if (pd.isna(df_main['show.network.country.name'][i])) == True:
                list_countries.append('multi_countries')
            else:
                list_countries.append(df_main['show.network.country.name'][i])
        else:
            list_countries.append(df_main['show.webChannel.country.name'][i])
    
    
    df_main['show.country'] = list_countries

    #Generos-Categorías
    df_generos = (pd.get_dummies(df_main['show.genres'].apply(pd.Series).stack()).sum(level=0)).reset_index()
    df_main = df_main.merge(df_generos, on = 'index', how = 'left') 

    #Días de la Semana
    df_days = (pd.get_dummies(df_main['show.schedule.days'].apply(pd.Series).stack()).sum(level=0)).reset_index()
    df_main=df_main.merge(df_days, on = 'index', how = 'left') 

    df_main = df_main.drop(['index', 'show.schedule.days', 'show.genres'], axis=1)

    # Perfilamiento de los datos
    reporte.generar_reporte(datos=df_main, titulo='reporte_perfilamiento_automatico', 
                            archivo='profiling/ perfilamiento_automatico_lulo_bank.html', 
                            secciones={'correlaciones': False, 
                                   'especificas': ['tipo', 'frecuencias', 
                                                   'duplicados_columnas', 
                                                  'descriptivas'], 
                                   'generales': True, 'muestra_datos': True})

    # Extracción Data CSV para Perfilamiento Manual
    df_main.to_csv('profiling/df_main.csv')

    # SQLite
    engine = create_engine('sqlite:///db/database.db', echo=False)
    
    df_schedule = df_main[['id', 'show.id' , 'url', 'name', 'season', 'type', 'airdate', 'airtime', 'airstamp', 'runtime', 'summary']]
    df_show_information = df_main[['show.id', 'show.url', 'show.name', 'show.type', 'show.language', 'show.runtime', 
                                    'show.averageRuntime', 'show.premiered', 'show.ended', 'show.officialSite', 'show.schedule.time',
                                    'show.rating.average', 'show.weight', 'show.webChannel.id', 'show.webChannel.name',
                                    'show.webChannel.country.name', 'show.webChannel.country.code',
                                    'show.webChannel.country.timezone', 'show.webChannel.officialSite',
                                    'show.dvdCountry', 'show.externals.tvrage', 'show.externals.thetvdb',
                                    'show.externals.imdb', 'show.image.medium', 'show.image.original',
                                    'show.summary', 'show.updated', 'show._links.self.href',
                                    'show._links.previousepisode.href', 'show.network.id',
                                    'show.network.name', 'show.network.country.name',
                                    'show.network.country.code', 'show.network.country.timezone',
                                    'show.network.officialSite', 'show._links.nextepisode.href',
                                    'show.webChannel', 'show.image', 'show.webChannel.country',
                                    'show.dvdCountry.name', 'show.dvdCountry.code',
                                    'show.dvdCountry.timezone', 'self.href', 'medium', 'original',
                                    'average', 'show.country']]
    df_show_categories = df_main[['show.id', 'Action', 'Adult', 'Adventure', 'Anime', 'Children',
                                  'Comedy', 'Crime', 'DIY', 'Drama', 'Family', 'Fantasy', 'Food',
                                  'History', 'Horror', 'Legal', 'Medical', 'Music', 'Mystery', 'Nature',
                                  'Romance', 'Science-Fiction', 'Sports', 'Supernatural', 'Thriller',
                                  'Travel', 'War']]

    df_show_days = df_main[['show.id','Friday', 'Monday', 'Saturday', 'Sunday', 'Thursday',
                           'Tuesday', 'Wednesday']]                               
    
    df_schedule.to_sql('schedule', con=engine)
    df_show_information.to_sql('shows', con=engine)
    df_show_categories.to_sql('shows_categories', con=engine)
    df_show_days.to_sql('shows_days', con=engine)



if __name__ == '__main__':
    run()

