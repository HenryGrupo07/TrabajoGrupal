import pandas as pd
import ast
import re

def only_dict(d):
    '''
    Convert json string representation of dictionary to a python dict
    A = json_normalize(df['columnA'].apply(only_dict).tolist()).add_prefix('columnA.')
    '''
    return ast.literal_eval(str(d))

def list_of_dicts(ld):
    '''
    Create a mapping of the tuples formed after 
    converting json strings of list to a python list
    B = json_normalize(df['columnB'].apply(list_of_dicts).tolist()).add_prefix('columnB.pos.') 
    '''
    return dict([(list(d.values())[1], list(d.values())[0]) for d in ast.literal_eval(ld)])

def formatear_columnas(df_columnas,df_to_merge):
    '''Esta funcion sirve para darle formato a los nombres de las columnas dinamizadas y luego hacer un merge con otro dataframe por su mismo indice'''
    df_columnas.columns = df_columnas.columns.str.strip()
    df_columnas.columns = df_columnas.columns.str.replace(" ","_")
    df_columnas.columns = df_columnas.columns.str.replace("&","")
    df_columnas.columns = df_columnas.columns.str.replace("__","_")
    df_final = df_to_merge.merge(df_columnas,left_index=True, right_index=True,how="left")
    return df_final

def get_gmap_id(url_series):
    df = pd.DataFrame()
    for url in url_series:
        df_json = pd.read_json(url, lines=True)
        df_json = df_json[["name","gmap_id","category"]]
        df_categorias = df_json.explode("category")
        df_categorias["category"] = df_categorias["category"].str.lower()
        df_categorias = df_categorias[(df_categorias["category"].str.contains('hotel|resort|inn|motel|lodg',regex = True))== True] #agregar lodge ganamos 2k sitios mas
        df = pd.concat([df,df_categorias])
    return df

def reduccion_categoria(categoria):
    if "dinner" in categoria:
        return "Dinner"
    elif "lodg" in categoria:
        return "Lodge"
    elif "motel" in categoria:
        return "Motel"
    elif "resort" in categoria:
        return "Resort"
    elif "inn" in categoria:
        return "Inn"
    elif "hotel" in categoria:
        return "Hotel"
    elif pd.isnull(categoria):
        return "Otros"
    else: return "Otros"

def list_to_lower(my_list):
    return [x.lower() for x in my_list]

def dinamizar_lista_a_columna(df,serie):
    lista_cat = df[serie].value_counts().index.to_list()
    flat_list = [item for sublist in lista_cat for item in sublist]
    for word in set(flat_list):
        if pd.isna(word):
            continue
        else: 
            df[word] = df[serie].dropna().apply(lambda x: 1 if word in x else 0)
            df[word].fillna(0,inplace=True)

def get_state(texto):
    if pd.isna(texto):
        return None
    else:
        pattern = re.compile(r'(\s{1}[A-Za-z]{2})(\s{1}\d{5})')
        text = re.search(pattern,texto)
        if bool(text):
            return text.group(1)

us_state_to_abbrev = {
'ALASKA':'AK',
'ARIZONA':'AZ',
'ARKANSAS':'AR',
'CALIFORNIA':'CA',
'COLORADO':'CO',
'CONNECTICUT':'CT',
'DELAWARE':'DE',
'FLORIDA':'FL',
'GEORGIA':'GA',
'HAWAII':'HI',
'IDAHO':'ID',
'ILLINOIS':'IL',
'INDIANA':'IN',
'IOWA':'IA',
'KANSAS':'KS',
'KENTUCKY':'KY',
'LOUISIANA':'LA',
'MAINE':'ME',
'MARYLAND':'MD',
'MASSACHUSETTS':'MA',
'MICHIGAN':'MI',
'MINNESOTA':'MN',
'MISSISSIPPI':'MS',
'MISSOURI':'MO',
'MONTANA':'MT',
'NEBRASKA':'NE',
'NEVADA':'NV',
'NEW HAMPSHIRE':'NH',
'NEW JERSEY':'NJ',
'NEW MEXICO':'NM',
'NEW YORK':'NY',
'NORTH CAROLINA':'NC',
'NORTH DAKOTA':'ND',
'OHIO':'OH',
'OKLAHOMA':'OK',
'OREGON':'OR',
'PENNSYLVANIA':'PA',
'RHODE ISLAND':'RI',
'SOUTH CAROLINA':'SC',
'SOUTH DAKOTA':'SD',
'TENNESSEE':'TN',
'TEXAS':'TX',
'UTAH':'UT',
'VERMONT':'VT',
'VIRGINIA':'VA',
'WASHINGTON':'WA',
'WEST VIRGINIA':'WV',
'WISCONSIN':'WI',
'WYOMING':'WY',
'ALABAMA':'AL',
'DISTRICT OF COLUMBIA':'DC'
}

abbrev_to_us_state = dict(map(reversed, us_state_to_abbrev.items()))

def get_descrip_state(texto):
    for i in us_state_to_abbrev.keys():
        if i in texto:
            return i
            exit