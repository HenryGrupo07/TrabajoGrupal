import streamlit as st
import pandas as pd
import numpy as np
from PIL import Image
from sklearn.preprocessing import OneHotEncoder
from sklearn.neighbors import NearestNeighbors
from transformers import AutoTokenizer, AutoModelForSequenceClassification
from scipy.special import softmax
import scipy


tokenizer_sbcBI = AutoTokenizer.from_pretrained("sbcBI/sentiment_analysis_model")
model_sbcBI = AutoModelForSequenceClassification.from_pretrained("sbcBI/sentiment_analysis_model")


st.set_page_config(layout="wide")
###################################

###########################################
df = pd.read_csv('df_streamlit.csv')
sentimiento = {'Positivo':1,'Neutro':0, 'Negativo':-1}
image = Image.open('scr/STREAMLIT.png')
positive = Image.open('scr/positive.png')
negative = Image.open('scr/negative.png')
neutral = Image.open('scr/neutral.png')
estados = ('Alabama', 'Alaska', 'Arizona', 'Arkansas', 'California',
       'Colorado', 'Connecticut', 'Delaware', 'District_of_Columbia',
       'Florida', 'Georgia', 'Hawaii', 'Idaho', 'Illinois', 'Indiana',
       'Iowa', 'Kansas', 'Kentucky', 'Louisiana', 'Maine', 'Maryland',
       'Massachusetts', 'Michigan', 'Minnesota', 'Mississippi',
       'Missouri', 'Montana', 'Nebraska', 'Nevada', 'New_Hampshire',
       'New_Jersey', 'New_Mexico', 'New_York', 'North_Carolina',
       'North_Dakota', 'Ohio', 'Oklahoma', 'Oregon', 'Pennsylvania',
       'Rhode_Island', 'South_Carolina', 'South_Dakota', 'Tennessee',
       'Texas', 'Utah', 'Vermont', 'Virginia', 'Washington',
       'West_Virginia', 'Wisconsin', 'Wyoming')
###################################################



###################################################
def polarity_sbcBI(sentence):
    encoded_text = tokenizer_sbcBI(sentence, return_tensors = 'pt')
    output = model_sbcBI(**encoded_text)
    scores = output[0][0].detach().numpy()
    scores = softmax(scores)
    #scores
    scores_dict = {
    'sbcBI_neg' : scores[0],
    'sbcBI_neu' : scores[1],
    'sbcBI_pos' : scores[2]}
      
    if scores_dict['sbcBI_pos'] > scores_dict['sbcBI_neg'] and scores_dict['sbcBI_pos'] > scores_dict['sbcBI_neu']:
        return 'positive'
    elif scores_dict['sbcBI_neg'] > scores_dict['sbcBI_pos'] and scores_dict['sbcBI_neg'] > scores_dict['sbcBI_neu']:
        return 'negative'
    else: return 'neutral'
#################################################################
sitios = df[["avg_rating","state_name","cat_name","sentimiento"]]
ohe_state = OneHotEncoder(sparse=False, handle_unknown="ignore")
ohe_state.fit(sitios[["state_name"]])

ohe_cat = OneHotEncoder(sparse=False, handle_unknown="ignore")
ohe_cat.fit(sitios[["cat_name"]])

dummies_cat = ohe_cat.transform(sitios[["cat_name"]])
headers_cat = np.array(ohe_cat.categories_).tolist()[0]
df_dummies_cat = pd.DataFrame(dummies_cat,columns=headers_cat)
sitios = pd.concat([sitios,df_dummies_cat],axis=1)
sitios.drop("cat_name",axis=1,inplace=True)

dummies_state = ohe_state.transform(sitios[["state_name"]])
headers_state = np.array(ohe_state.categories_).tolist()[0]
df_dummies_state = pd.DataFrame(dummies_state,columns=headers_state)
sitios = pd.concat([sitios,df_dummies_state],axis=1)
sitios.drop("state_name",axis=1,inplace=True)

sitios_sparse = scipy.sparse.csr_matrix(sitios)

from sklearn.neighbors import NearestNeighbors
n_neighbors=6
nneighbors = NearestNeighbors(n_neighbors = n_neighbors, metric = 'cosine').fit(sitios_sparse)
###################################################################

#parametros de entrada dinamicos para streamlit
def get_sparse_evaluardo(v_avg_rating,v_state_name,v_cat_name,v_sentimiento):
    #consilidamos parametros en df
    evaluar_recomend = pd.DataFrame({
                    "avg_rating":v_avg_rating,
                    "state_name":v_state_name,
                    "cat_name":v_cat_name,
                    "sentimiento":v_sentimiento
                    },index=[0])

    #creamos las columnas dummies para poder ingresar al modelo
    encode_state = ohe_state.transform(pd.DataFrame({"state_name":v_state_name},index=[0]))
    df_state_dummies_v = pd.DataFrame(encode_state,columns=headers_state)

    encode_cat = ohe_cat.transform(pd.DataFrame({"cat_name":v_cat_name},index=[0]))
    df_cat_dummies_v = pd.DataFrame(encode_cat,columns=headers_cat)

    evaluar_recomend = pd.concat([evaluar_recomend,df_cat_dummies_v,df_state_dummies_v],axis=1)
        
    evaluar_recomend.drop(["cat_name","state_name"],axis=1,inplace=True)

    evaluar_recomend_sparse = scipy.sparse.csr_matrix(evaluar_recomend)

    return evaluar_recomend_sparse

category = st.sidebar.selectbox("**Seleccionar la categoría del alojamiento**",("Hotel", "Motel","Resort","Inn","Lodge"))
sentiment = st.sidebar.selectbox("**Seleccionar tipo de sentimiento**",("Positivo", "Neutro","Negativo"))
states = st.sidebar.selectbox("**Seleccionar el estado**",estados)
rating = st.sidebar.select_slider("**Seleccionar el rating**",options=[1, 2, 3, 4, 5])



####################
### INTRODUCTION ###
####################
col13,col14= st.columns([1,5])
with col13:
    st.write("")
with col14:    
    st.image(image,width=900)   

evaluador = get_sparse_evaluardo(rating,states,category,sentimiento[sentiment])
dif, ind = nneighbors.kneighbors(evaluador)
recomen =df.loc[ind[0][1:], :]
map = recomen[['latitude','longitude']]
with open('style.css') as f:
    st.markdown(f'<style>{f.read()}</style>', unsafe_allow_html=True)
col9, col10, col11 = st.columns(3)    
with col10:
    st.markdown('<h3 style="text-align: justify;color: white">Sistema de recomendación</h3>', unsafe_allow_html=True)   
col1,col2= st.columns(2)
with col1:
    st.markdown('<h3 style="text-align: justify;color: white">Top 5 de alojamientos recomendados</h3>', unsafe_allow_html=True)
    st.dataframe(data=recomen[['name_hotel',"avg_rating",'cat_name','state_name','sentimiento','url']])
with col2: 
    
    st.map(map)
    
st.write("")
col6, col7, col8 = st.columns(3)
with col7:
    st.markdown('<h3 style="text-align: justify;color: white">Análisis de sentimientos</h3>', unsafe_allow_html=True)
 
col3, col4, col5 = st.columns([1,2,1])
with col4:
    #st.markdown('<h3 style="text-align: justify;">your sentiment about the website</h3>', unsafe_allow_html=True)
    title = st.text_input('Your feeling about the website 👇')

    sent = polarity_sbcBI(title)
   
    if sent == 'positive':
        st.image(positive,width=100) 
    elif sent == 'negative':
        st.image(negative,width=100) 
    else: st.image(neutral,width=100)    
    

