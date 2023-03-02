import streamlit as st
from PIL import Image



st.set_page_config(layout="wide")
with open('style.css') as f:
    st.markdown(f'<style>{f.read()}</style>', unsafe_allow_html=True)
image = Image.open('scr/STREAMLIT.png')
alcance = Image.open('scr/ALCANCE.png')
scrum = Image.open('scr/SCRUM.png')
canvas = Image.open('scr/CANVAS.png')
arquitectura = Image.open('scr/ARQUITECTURA.png')
fuentes = Image.open('scr/FUENTES.png')
modelo = Image.open('scr/MODELO.png')
####################################

####################
### INTRODUCTION ###
####################



col1,col2= st.columns([1,5])
with col1:
    st.write("")
with col2:    
    st.image(image,width=900)
    
    

#########################
#category = st.sidebar.selectbox("**Seleccionar la categoría del alojamiento**",("Hotel", "Motel","Resort","Inn","Lodge"))
#st.sidebar.button('Alcance')

st.write("")
st.write("")
col3,col4= st.columns([1,5])
with col3:
    st.write("")
with col4: 
      
    if st.sidebar.button('Alcance'):
        st.image(alcance,width=900)
    if st.sidebar.button('Fuentes'):
        st.image(fuentes,width=900)
    if st.sidebar.button('Propuesta de valor'):
        st.image(canvas,width=900)
    if st.sidebar.button('SCRUM'):
        st.image(scrum,width=900)
    if st.sidebar.button('Arquitectura'):
        st.image(arquitectura,width=900)
    if st.sidebar.button('Modelos'):
        st.image(modelo,width=900)
        

